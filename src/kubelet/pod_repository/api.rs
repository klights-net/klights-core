//! `PodApiService` — the API-handler-facing pod create/update/patch/delete
//! pipeline (today the body of `src/pod_create.rs::create_pod_through_pipeline`
//! and the Pod-specific arms of `namespaced_resource_handlers!`).
//!
//! Holds `Arc<PodStore>` for every `("v1","Pod",...)` write, plus
//! `DatastoreHandle` for the non-Pod admission/quota/limitrange helpers
//! that touch other kinds. `Arc<PodWorkqueue>` carries deferred
//! UID-bound actor wakeups; the API service never calls
//! `TaskSupervisor::spawn_delay` directly. `Arc<SideEffectRegistry>` and
//! `Arc<SideEffectMetrics>` are reserved for post-write hooks.
//!
//! Implementations land in Tasks 11 (create), 12 (update/patch), and 13
//! (delete + delete-collection).

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::api::{
    AdmissionContextRequest, AppError, apply_limitrange_defaults_to_pod, apply_patch,
    apply_pod_runtimeclass_admission, apply_pod_service_account_defaults,
    apply_pod_spec_create_defaults, build_admission_context, check_resource_quota_for_creation,
    check_resource_quota_for_pod_update, compute_qos_class, enforce_limitrange_constraints_for_pod,
    enforce_pod_security_admission, normalize_resource_for_storage, resolve_resource_name,
    run_admission_for_request, validate_builtin_resource_spec, validate_dns_subdomain,
    validate_pod_resource_requirements_immutable, validate_pod_sysctls,
};
use crate::control_plane::client::LeaderApiClient;
use crate::datastore::{DatastoreHandle, Resource, ResourcePreconditions};
use crate::side_effects::{SideEffectMetrics, SideEffectRegistry};
use crate::task_supervisor::TaskSupervisor;

use crate::api::DeleteOptions;

use super::state_only_writer::StateOnlyWriter;
use super::store::{PodStore, preserve_status_from_current};
use super::types::{
    PodApiCreateRequest, PodApiCreateResult, PodApiDeleteOutcome, PodApiUpdateOutcome,
    PodStatusPatchType,
};
use super::workqueue::PodWorkqueue;

pub(super) const SCHED_BIND_CONCURRENCY: usize = 8;

#[cfg(test)]
pub(super) struct SchedulerBindGateForTest {
    entered: std::sync::atomic::AtomicUsize,
    entered_notify: tokio::sync::Notify,
    release_notify: tokio::sync::Notify,
}

#[cfg(test)]
impl SchedulerBindGateForTest {
    pub fn new() -> Self {
        Self {
            entered: std::sync::atomic::AtomicUsize::new(0),
            entered_notify: tokio::sync::Notify::new(),
            release_notify: tokio::sync::Notify::new(),
        }
    }

    pub async fn wait_for_entered_at_least(&self, target: usize) {
        loop {
            if self.entered.load(std::sync::atomic::Ordering::SeqCst) >= target {
                return;
            }
            self.entered_notify.notified().await;
        }
    }

    pub fn release_all(&self) {
        self.release_notify.notify_waiters();
    }

    async fn enter_and_wait(&self) {
        self.entered
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered_notify.notify_waiters();
        self.release_notify.notified().await;
    }
}

fn ensure_resource_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), AppError> {
    if let Some(expected_uid) = preconditions.uid.as_deref() {
        let actual_uid = resource
            .data
            .pointer("/metadata/uid")
            .and_then(|v| v.as_str());
        if actual_uid != Some(expected_uid) {
            return Err(AppError::Conflict("UID precondition failed".to_string()));
        }
    }
    if let Some(expected_rv) = preconditions.resource_version
        && resource.resource_version != expected_rv
    {
        return Err(AppError::Conflict(
            "resourceVersion precondition failed".to_string(),
        ));
    }
    Ok(())
}

async fn apply_pod_service_account_admission(
    db: &DatastoreHandle,
    namespace: &str,
    body: &mut Value,
) -> Result<(), AppError> {
    let Some(spec_obj) = body.pointer_mut("/spec").and_then(|v| v.as_object_mut()) else {
        return Ok(());
    };

    apply_pod_service_account_defaults(spec_obj);

    let image_pull_secrets_empty = spec_obj
        .get("imagePullSecrets")
        .and_then(|v| v.as_array())
        .is_none_or(Vec::is_empty);
    if !image_pull_secrets_empty {
        return Ok(());
    }

    let service_account_name = spec_obj
        .get("serviceAccountName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or("default")
        .to_string();
    let Some(service_account) = db
        .get_resource(
            "v1",
            "ServiceAccount",
            Some(namespace),
            &service_account_name,
        )
        .await?
    else {
        return Ok(());
    };
    let Some(image_pull_secrets) = service_account
        .data
        .get("imagePullSecrets")
        .and_then(|v| v.as_array())
        .filter(|secrets| !secrets.is_empty())
        .cloned()
    else {
        return Ok(());
    };

    spec_obj.insert(
        "imagePullSecrets".to_string(),
        Value::Array(image_pull_secrets),
    );
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PodSchedulingDecision {
    node_name: Option<String>,
    pending: bool,
    unschedulable_message: Option<String>,
    preemption_victims: Vec<PreemptionVictim>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodSchedulingMode {
    InlineSingleNode,
    DeferredMultiNodeLeader,
}

#[derive(Clone)]
pub struct PodRepositoryBuildConfig {
    pub db: DatastoreHandle,
    pub supervisor: Arc<TaskSupervisor>,
    pub side_effects: Arc<SideEffectRegistry>,
    pub metrics: Arc<SideEffectMetrics>,
    pub network_events: crate::networking::pod_network_events::PodNetworkEvents,
    pub scheduling_mode: PodSchedulingMode,
    pub outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    pub cluster_api: Option<Arc<dyn LeaderApiClient>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreemptionVictim {
    namespace: String,
    name: String,
    resource_version: i64,
    data: Value,
}

struct PreemptionApplyContext<'a> {
    store: &'a PodStore,
    workqueue: &'a Arc<PodWorkqueue>,
    side_effects: &'a SideEffectRegistry,
    metrics: &'a SideEffectMetrics,
    preemptor_namespace: &'a str,
    preemptor_name: &'a str,
}

pub struct PodApiService {
    store: Arc<PodStore>,
    status_only: Arc<dyn StateOnlyWriter>,
    db: DatastoreHandle,
    supervisor: Arc<TaskSupervisor>,
    workqueue: Arc<PodWorkqueue>,
    side_effects: Arc<SideEffectRegistry>,
    metrics: Arc<SideEffectMetrics>,
    outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
    #[cfg(test)]
    scheduler_bind_gate: std::sync::Mutex<Option<Arc<SchedulerBindGateForTest>>>,
}

pub struct PodApiServiceDependencies {
    pub store: Arc<PodStore>,
    pub status_only: Arc<dyn StateOnlyWriter>,
    pub db: DatastoreHandle,
    pub supervisor: Arc<TaskSupervisor>,
    pub workqueue: Arc<PodWorkqueue>,
    pub side_effects: Arc<SideEffectRegistry>,
    pub metrics: Arc<SideEffectMetrics>,
    pub outbox: Option<Arc<crate::kubelet::outbox::Outbox>>,
}

impl PodApiService {
    async fn enqueue_actor_finalize_if_ready(&self, ns: &str, name: &str, resource: &Resource) {
        if resource
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_none()
            || resource
                .data
                .pointer("/metadata/finalizers")
                .and_then(|finalizers| finalizers.as_array())
                .is_some_and(|finalizers| !finalizers.is_empty())
        {
            return;
        }

        if let Err(err) = self
            .workqueue
            .enqueue_deferred_delete_with_target_node(
                ns.to_string(),
                name.to_string(),
                resource.uid.clone(),
                Duration::ZERO,
                pod_target_node_from_pod_data(&resource.data),
            )
            .await
        {
            self.metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                namespace = %ns,
                name = %name,
                uid = %resource.uid,
                error = %err,
                "failed to enqueue actor finalization after pod finalizers drained"
            );
        }
    }

    pub fn new(dependencies: PodApiServiceDependencies) -> Self {
        let PodApiServiceDependencies {
            store,
            status_only,
            db,
            supervisor,
            workqueue,
            side_effects,
            metrics,
            outbox,
        } = dependencies;
        Self {
            store,
            status_only,
            db,
            supervisor,
            workqueue,
            side_effects,
            metrics,
            outbox,
            #[cfg(test)]
            scheduler_bind_gate: std::sync::Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub(super) fn set_scheduler_bind_gate_for_test(&self, gate: Arc<SchedulerBindGateForTest>) {
        *self.scheduler_bind_gate.lock().unwrap() = Some(gate);
    }

    #[cfg(test)]
    async fn wait_scheduler_bind_gate_for_test(&self) {
        let gate = self.scheduler_bind_gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.enter_and_wait().await;
        }
    }

    #[cfg(not(test))]
    async fn wait_scheduler_bind_gate_for_test(&self) {}

    /// Body of `src/pod_create.rs::create_pod_through_pipeline`, moved
    /// into the repository. The single `("v1","Pod",...)` DB call is the
    /// final `store.create(...)` — every other DB touch is admission,
    /// quota, or limit-range helpers against other kinds, which legitimately
    /// flow through `self.db`.
    pub async fn api_create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> Result<PodApiCreateResult, AppError> {
        let PodApiCreateRequest {
            namespace,
            name,
            mut body,
            dry_run,
            run_admission,
        } = request;

        crate::api::reject_if_namespace_missing_or_terminating(self.db.as_ref(), &namespace)
            .await?;

        if let Err(msg) = crate::kubelet::volumes::validate_volume_subpaths(&body) {
            return Err(AppError::UnprocessableEntity(msg));
        }
        if let Err(msg) = crate::kubelet::volumes::validate_volume_projection_paths(&body) {
            return Err(AppError::UnprocessableEntity(msg));
        }
        validate_pod_sysctls(&body)?;

        if run_admission {
            body = run_admission_for_request(
                self.db.as_ref(),
                build_admission_context(AdmissionContextRequest {
                    api_version: "v1",
                    kind: "Pod",
                    operation: "CREATE",
                    namespace: Some(namespace.clone()),
                    name: body
                        .get("metadata")
                        .and_then(|m| m.get("name"))
                        .and_then(|n| n.as_str())
                        .map(ToString::to_string),
                    object: body,
                    old_object: None,
                    dry_run,
                    subresource: None,
                    options: None,
                }),
            )
            .await?;
        }

        apply_pod_runtimeclass_admission(self.db.as_ref(), &mut body).await?;
        apply_limitrange_defaults_to_pod(self.db.as_ref(), &namespace, &mut body).await?;
        enforce_limitrange_constraints_for_pod(self.db.as_ref(), &namespace, &body).await?;
        validate_builtin_resource_spec("Pod", &body)?;
        apply_pod_service_account_admission(&self.db, &namespace, &mut body).await?;

        if dry_run {
            return Ok(PodApiCreateResult {
                resource: None,
                body,
            });
        }

        check_resource_quota_for_creation(self.db.as_ref(), &namespace, "Pod", &body).await?;

        let resource_name = if name.trim().is_empty() {
            resolve_resource_name(&mut body)?
        } else {
            name
        };
        if !validate_dns_subdomain(&resource_name) {
            return Err(AppError::UnprocessableEntity(format!(
                "Invalid metadata.name '{}': must be a valid DNS subdomain (lowercase alphanumeric, hyphens, dots; max 253 chars; cannot start/end with hyphen or dot)",
                resource_name
            )));
        }

        if let Some(obj) = body.as_object_mut()
            && let Some(metadata) = obj.get_mut("metadata")
            && let Some(meta_obj) = metadata.as_object_mut()
        {
            meta_obj.insert("namespace".to_string(), Value::String(namespace.clone()));
            meta_obj.insert("name".to_string(), Value::String(resource_name.clone()));

            let uid_missing_or_empty = meta_obj
                .get("uid")
                .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()));
            if uid_missing_or_empty {
                meta_obj.insert(
                    "uid".to_string(),
                    Value::String(uuid::Uuid::new_v4().to_string()),
                );
            }
            if meta_obj
                .get("creationTimestamp")
                .is_none_or(|v| v.is_null())
            {
                meta_obj.insert(
                    "creationTimestamp".to_string(),
                    Value::String(crate::utils::k8s_timestamp()),
                );
            }
            let generation = meta_obj
                .get("generation")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            if generation == 0 {
                meta_obj.insert("generation".to_string(), json!(1));
            }
        }

        apply_priority_class_to_pod(self.db.as_ref(), &mut body).await?;

        let qos_class = compute_qos_class(&body);
        let scheduling_decision = initial_create_scheduling_decision(&body);
        if let Some(obj) = body.as_object_mut() {
            let spec = obj.entry("spec".to_string()).or_insert_with(|| json!({}));
            if let Some(spec_obj) = spec.as_object_mut() {
                apply_pod_spec_create_defaults(spec_obj);
                let needs_node_name = spec_obj
                    .get("nodeName")
                    .map(|v| v.as_str().map(|s| s.is_empty()).unwrap_or(v.is_null()))
                    .unwrap_or(true);
                if needs_node_name {
                    if let Some(scheduled_node) = scheduling_decision.node_name.as_deref() {
                        spec_obj.insert("nodeName".to_string(), json!(scheduled_node));
                    } else {
                        spec_obj.remove("nodeName");
                    }
                }
            }
            let pod_scheduled_condition = if scheduling_decision.pending {
                json!({
                    "type": "PodScheduled",
                    "status": "False",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                    "reason": "SchedulingPending",
                })
            } else if let Some(message) = scheduling_decision.unschedulable_message.as_deref() {
                json!({
                    "type": "PodScheduled",
                    "status": "False",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                    "reason": "Unschedulable",
                    "message": message,
                })
            } else {
                json!({
                    "type": "PodScheduled",
                    "status": "True",
                    "lastTransitionTime": crate::utils::k8s_timestamp(),
                })
            };
            tracing::info!(
                namespace = %namespace,
                name = %resource_name,
                "pod-lifecycle: WRITE 1 — api_create_pod writing initial Pending status"
            );
            obj.insert(
                "status".to_string(),
                json!({
                    "phase": "Pending",
                    "conditions": [
                        {
                            "type": "Initialized",
                            "status": "True",
                            "lastTransitionTime": crate::utils::k8s_timestamp(),
                        },
                        {
                            "type": "Ready",
                            "status": "False",
                            "lastTransitionTime": crate::utils::k8s_timestamp(),
                        },
                        {
                            "type": "ContainersReady",
                            "status": "False",
                            "lastTransitionTime": crate::utils::k8s_timestamp(),
                        },
                        pod_scheduled_condition
                    ],
                    "containerStatuses": [],
                    "qosClass": qos_class,
                }),
            );
        }

        normalize_resource_for_storage("v1", "Pod", &mut body);
        enforce_pod_security_admission(self.db.as_ref(), &namespace, &body).await?;
        let resource = self
            .store
            .create(&namespace, &resource_name, body)
            .await
            .map_err(|e| -> AppError { e.into() })?;
        if let Err(e) = crate::controllers::gc::reconcile_owner_references(
            self.db.as_ref(),
            resource.clone(),
            self as &dyn crate::controllers::gc::GcPodDeleteSink,
        )
        .await
        {
            tracing::warn!(
                namespace = %namespace,
                name = %resource_name,
                error = %e,
                "controller pod ownerReference GC reconciliation failed"
            );
        }
        let response_body: Value = (*resource.data).clone();
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_create(
            &resource.data,
            self.store.db().as_ref(),
            &self.side_effects.controller_dispatcher_slot(),
        )
        .await
        {
            tracing::debug!(
                target: "klights::pod_repository::api",
                error = %err,
                "failed to enqueue Service reconcile after pod create"
            );
        }
        Ok(PodApiCreateResult {
            resource: Some(resource),
            body: response_body,
        })
    }

    pub async fn schedule_all_unbound_pods(self: &Arc<Self>) -> Result<(), AppError> {
        let initial = self
            .store
            .list(None, None, None, None, None)
            .await
            .map_err(|e| -> AppError { e.into() })?;
        let candidates = sorted_unbound_pods(initial.items);

        for wave in candidates.chunks(SCHED_BIND_CONCURRENCY) {
            let snapshot = self.scheduler_snapshot().await?;
            let mut reservations = Vec::new();
            let mut handles = Vec::with_capacity(wave.len());

            for pod in wave {
                let namespace = pod_namespace(pod);
                let name = pod.name.clone();
                let decision = schedule_pod_from_snapshot(
                    self.store.as_ref(),
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

                let api = self.clone();
                let handle = self
                    .supervisor
                    .spawn_async(
                        crate::task_supervisor::TaskCategory::Background,
                        format!("scheduler_bind/{namespace}/{name}"),
                        async move {
                            api.schedule_pending_pod_with_decision(&namespace, &name, decision)
                                .await
                        },
                    )
                    .await
                    .map_err(|e| AppError::Internal(e.to_string()))?;
                handles.push(handle);
            }

            for handle in handles {
                handle.join().await.map_err(|e| {
                    AppError::Internal(format!("scheduler bind task failed: {e}"))
                })??;
            }
        }

        Ok(())
    }

    async fn scheduler_snapshot(&self) -> Result<SchedulerSnapshot, AppError> {
        let nodes = self
            .db
            .list_resources(
                "v1",
                "Node",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let pods = self
            .store
            .list(None, None, None, None, None)
            .await
            .map_err(|e| -> AppError { e.into() })?;
        let namespaces = self
            .db
            .list_resources(
                "v1",
                "Namespace",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        let pdbs = self
            .db
            .list_resources(
                "policy/v1",
                "PodDisruptionBudget",
                None,
                crate::datastore::ResourceListQuery::all(),
            )
            .await?;
        Ok(SchedulerSnapshot {
            nodes: nodes.items,
            pods: pods.items,
            namespaces: namespaces.items,
            pdbs: pdbs.items,
        })
    }

    pub async fn schedule_pending_pod(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<Resource>, AppError> {
        let Some(current) = self
            .store
            .get(namespace, name)
            .await
            .map_err(|e| -> AppError { e.into() })?
        else {
            return Ok(None);
        };
        if current
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
        {
            return Ok(Some(current));
        }

        let decision = schedule_pod_on_available_nodes(
            self.store.as_ref(),
            self.db.as_ref(),
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
    ) -> Result<Option<Resource>, AppError> {
        let Some(current) = self
            .store
            .get(namespace, name)
            .await
            .map_err(|e| -> AppError { e.into() })?
        else {
            return Ok(None);
        };
        if current
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_some_and(|s| !s.is_empty())
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
    ) -> Result<Option<Resource>, AppError> {
        if let Some(node_name) = decision.node_name.as_deref()
            && !self
                .planned_node_still_fits(namespace, name, &current.data, node_name)
                .await?
        {
            decision = PodSchedulingDecision {
                node_name: None,
                pending: false,
                unschedulable_message: Some(
                    "node allocation changed before scheduler bind".to_string(),
                ),
                preemption_victims: Vec::new(),
            };
        }

        let mut body: Value = (*current.data).clone();
        if let Some(spec) = body.get_mut("spec").and_then(|v| v.as_object_mut()) {
            match decision.node_name.as_deref() {
                Some(node_name) => {
                    spec.insert("nodeName".to_string(), json!(node_name));
                }
                None => {
                    spec.remove("nodeName");
                }
            }
        }
        if let Some(status) = body.get_mut("status").and_then(|v| v.as_object_mut()) {
            let conditions = status
                .entry("conditions".to_string())
                .or_insert_with(|| json!([]));
            if let Some(conditions) = conditions.as_array_mut() {
                conditions.retain(|condition| {
                    condition.get("type").and_then(|v| v.as_str()) != Some("PodScheduled")
                });
                conditions.push(
                    if let Some(message) = decision.unschedulable_message.as_deref() {
                        json!({
                            "type": "PodScheduled",
                            "status": "False",
                            "lastTransitionTime": crate::utils::k8s_timestamp(),
                            "reason": "Unschedulable",
                            "message": message,
                        })
                    } else {
                        json!({
                            "type": "PodScheduled",
                            "status": "True",
                            "lastTransitionTime": crate::utils::k8s_timestamp(),
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
            self.store
                .update_including_status_for_scheduler(
                    namespace,
                    name,
                    body,
                    current.resource_version,
                )
                .await
                .map_err(|e| -> AppError { e.into() })?
        } else if spec_changed {
            self.store
                .update(namespace, name, body, current.resource_version)
                .await
                .map_err(|e| -> AppError { e.into() })?
        } else {
            current
        };
        if status_changed && !spec_changed {
            let status = desired_status.expect("status_changed requires desired status");
            final_resource = self
                .status_only
                .write_status(
                    namespace,
                    name,
                    status,
                    Some(final_resource.resource_version),
                )
                .await
                .map_err(|e| -> AppError { e.into() })?;
        }
        self.wait_scheduler_bind_gate_for_test().await;
        apply_preemption_victims(
            PreemptionApplyContext {
                store: self.store.as_ref(),
                workqueue: &self.workqueue,
                side_effects: self.side_effects.as_ref(),
                metrics: self.metrics.as_ref(),
                preemptor_namespace: namespace,
                preemptor_name: name,
            },
            &decision.preemption_victims,
        )
        .await?;
        if status_changed
            && let Some(message) = decision.unschedulable_message.as_deref()
            && let Err(e) = crate::kubelet::events::emit_pod_event_with_outbox(
                self.db.as_ref(),
                self.outbox.as_deref(),
                crate::kubelet::events::PodEventRecord {
                    pod: &final_resource.data,
                    reason: "FailedScheduling",
                    message,
                    event_type: "Warning",
                    reporting_component: "default-scheduler",
                    reporting_instance: decision.node_name.as_deref().unwrap_or(""),
                },
            )
            .await
        {
            tracing::warn!(
                namespace = %namespace,
                name = %name,
                error = %e,
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
    ) -> Result<bool, AppError> {
        let live_decision = schedule_pod_on_available_nodes(
            self.store.as_ref(),
            self.db.as_ref(),
            pod,
            namespace,
            name,
        )
        .await?;
        Ok(live_decision.node_name.as_deref() == Some(planned_node))
    }

    pub async fn bind_pod_from_api(
        &self,
        namespace: &str,
        name: &str,
        binding: Value,
        dry_run: bool,
    ) -> Result<(), AppError> {
        validate_pod_binding_object(namespace, name, &binding)?;
        let target_node = binding
            .pointer("/target/name")
            .and_then(|v| v.as_str())
            .expect("validate_pod_binding_object requires target.name")
            .to_string();

        let current = self
            .store
            .get(namespace, name)
            .await
            .map_err(|e| -> AppError { e.into() })?
            .ok_or_else(|| AppError::NotFound(format!("pods \"{}\" not found", name)))?;
        ensure_resource_preconditions_match(&current, &binding_resource_preconditions(&binding)?)?;
        if current
            .data
            .pointer("/metadata/deletionTimestamp")
            .is_some_and(|v| !v.is_null())
        {
            return Err(AppError::Conflict(format!(
                "pod {namespace}/{name} is being deleted"
            )));
        }
        if current
            .data
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .is_some_and(|node_name| !node_name.is_empty())
        {
            return Err(AppError::Conflict(format!(
                "pod {namespace}/{name} is already assigned to a node"
            )));
        }
        if current
            .data
            .pointer("/spec/schedulingGates")
            .and_then(|v| v.as_array())
            .is_some_and(|gates| !gates.is_empty())
        {
            return Err(AppError::Conflict(format!(
                "pod {namespace}/{name} has scheduling gates"
            )));
        }

        let mut body: Value = (*current.data).clone();
        merge_binding_annotations(&mut body, &binding);
        set_bound_node_name(&mut body, &target_node)?;
        upsert_pod_scheduled_true(&mut body)?;

        if dry_run {
            return Ok(());
        }

        self.store
            .update_including_status_for_scheduler(namespace, name, body, current.resource_version)
            .await
            .map_err(|e| -> AppError { e.into() })?;
        Ok(())
    }

    /// Body of the macro's Pod-update branch (today inlined into
    /// `pod_handlers::update_pod`). Runs Pod-specific validation,
    /// admission, immutability + quota checks, normalization, then
    /// persists via `store.update(...)` with CAS. The handler keeps the
    /// post-update side-effect calls (`maybe_hard_delete_pod_after_finalizers_drained`,
    /// `reconcile_owner_refs_after_mutation`, `state.side_effects.run_hooks`).
    pub async fn api_update_pod(
        &self,
        ns: &str,
        name: &str,
        mut body: Value,
        current: Resource,
        dry_run: bool,
    ) -> Result<PodApiUpdateOutcome, AppError> {
        if let Err(msg) = crate::kubelet::volumes::validate_volume_subpaths(&body) {
            return Err(AppError::UnprocessableEntity(msg));
        }
        if let Err(msg) = crate::kubelet::volumes::validate_volume_projection_paths(&body) {
            return Err(AppError::UnprocessableEntity(msg));
        }
        validate_pod_sysctls(&body)?;

        body = run_admission_for_request(
            self.db.as_ref(),
            build_admission_context(AdmissionContextRequest {
                api_version: "v1",
                kind: "Pod",
                operation: "UPDATE",
                namespace: Some(ns.to_string()),
                name: Some(name.to_string()),
                object: body,
                old_object: Some((*current.data).clone()),
                dry_run,
                subresource: None,
                options: None,
            }),
        )
        .await?;

        validate_pod_resource_requirements_immutable(&current.data, &body)?;
        check_resource_quota_for_pod_update(self.db.as_ref(), ns, &current.data, &body).await?;
        validate_builtin_resource_spec("Pod", &body)?;

        normalize_resource_for_storage("v1", "Pod", &mut body);
        preserve_status_from_current(&current.data, &mut body);
        enforce_pod_security_admission(self.db.as_ref(), ns, &body).await?;

        if dry_run {
            return Ok(PodApiUpdateOutcome::DryRun(body));
        }

        let resource = self
            .store
            .update(ns, name, body, current.resource_version)
            .await
            .map_err(|e| -> AppError { e.into() })?;
        let previous = std::sync::Arc::unwrap_or_clone(current.data);
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &resource.data,
            self.store.db().as_ref(),
            &self.side_effects.controller_dispatcher_slot(),
        )
        .await
        {
            tracing::debug!(
                target: "klights::pod_repository::api",
                error = %err,
                "failed to enqueue Service reconcile after pod endpoint state changed"
            );
        }
        self.enqueue_actor_finalize_if_ready(ns, name, &resource)
            .await;
        Ok(PodApiUpdateOutcome::Persisted(resource))
    }

    /// Body of the macro's Pod-patch branch. Handles SSA-create when
    /// `patch_type == ApplyPatch` against a missing pod (matches today's
    /// generic handler). Other patch types against a missing pod return
    /// 404. Includes the existing 20-attempt retry-on-409 loop with
    /// capped exponential backoff via `TaskSupervisor::sleep`.
    pub async fn api_patch_pod(
        &self,
        ns: &str,
        name: &str,
        patch: Value,
        patch_type: PodStatusPatchType,
        dry_run: bool,
    ) -> Result<PodApiUpdateOutcome, AppError> {
        let content_type = patch_type_to_content_type(patch_type);

        // SSA-create on missing pod (only ApplyPatch).
        if matches!(patch_type, PodStatusPatchType::ApplyPatch) {
            let exists = self.store.get(ns, name).await?.is_some();
            if !exists {
                let result = self
                    .api_create_pod(PodApiCreateRequest {
                        namespace: ns.to_string(),
                        name: name.to_string(),
                        body: patch,
                        dry_run,
                        run_admission: true,
                    })
                    .await?;
                return Ok(match result.resource {
                    Some(resource) => PodApiUpdateOutcome::Persisted(resource),
                    None => PodApiUpdateOutcome::DryRun(result.body),
                });
            }
        }

        let max_retries = 20u32;
        for attempt in 0..max_retries {
            let current = self
                .store
                .get(ns, name)
                .await?
                .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;

            let mut patched = apply_patch(&current.data, &patch, Some(content_type))?;

            // SSA: store the applied configuration in the
            // kubectl.kubernetes.io/last-applied-configuration annotation.
            if matches!(patch_type, PodStatusPatchType::ApplyPatch)
                && let Some(obj) = patched.as_object_mut()
            {
                let patch_str = serde_json::to_string(&patch).unwrap_or_default();
                let meta = obj.entry("metadata").or_insert_with(|| json!({}));
                if let Some(meta_obj) = meta.as_object_mut() {
                    let annot = meta_obj.entry("annotations").or_insert_with(|| json!({}));
                    if let Some(annot_obj) = annot.as_object_mut() {
                        annot_obj.insert(
                            "kubectl.kubernetes.io/last-applied-configuration".to_string(),
                            json!(patch_str),
                        );
                    }
                }
            }

            patched = run_admission_for_request(
                self.db.as_ref(),
                build_admission_context(AdmissionContextRequest {
                    api_version: "v1",
                    kind: "Pod",
                    operation: "UPDATE",
                    namespace: Some(ns.to_string()),
                    name: Some(name.to_string()),
                    object: patched,
                    old_object: Some((*current.data).clone()),
                    dry_run,
                    subresource: None,
                    options: None,
                }),
            )
            .await?;

            validate_pod_resource_requirements_immutable(&current.data, &patched)?;
            check_resource_quota_for_pod_update(self.db.as_ref(), ns, &current.data, &patched)
                .await?;
            validate_builtin_resource_spec("Pod", &patched)?;

            normalize_resource_for_storage("v1", "Pod", &mut patched);
            preserve_status_from_current(&current.data, &mut patched);
            enforce_pod_security_admission(self.db.as_ref(), ns, &patched).await?;

            if dry_run {
                return Ok(PodApiUpdateOutcome::DryRun(patched));
            }

            match self
                .store
                .update(ns, name, patched, current.resource_version)
                .await
            {
                Ok(resource) => {
                    let previous = std::sync::Arc::unwrap_or_clone(current.data);
                    if let Err(err) =
                        crate::side_effects::service_pod::enqueue_services_after_pod_update(
                            &previous,
                            &resource.data,
                            self.store.db().as_ref(),
                            &self.side_effects.controller_dispatcher_slot(),
                        )
                        .await
                    {
                        tracing::debug!(
                            target: "klights::pod_repository::api",
                            error = %err,
                            "failed to enqueue Service reconcile after pod endpoint state changed"
                        );
                    }
                    self.enqueue_actor_finalize_if_ready(ns, name, &resource)
                        .await;
                    return Ok(PodApiUpdateOutcome::Persisted(resource));
                }
                Err(e)
                    if attempt + 1 < max_retries
                        && crate::datastore::errors::is_conflict_error(&e) =>
                {
                    let backoff_ms = std::cmp::min(20u64.saturating_mul(1u64 << attempt), 250);
                    let _ = self
                        .supervisor
                        .sleep(
                            "patch_conflict_retry_backoff",
                            Duration::from_millis(backoff_ms),
                        )
                        .await;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }

        unreachable!("PATCH retry loop exhausted without returning");
    }

    /// Body of the macro's Pod-delete branch. Sets
    /// `metadata.deletionTimestamp` (only if absent) +
    /// `metadata.deletionGracePeriodSeconds` (from `options`, else the Pod
    /// spec, else default 30), persists the MODIFIED event, and records a
    /// UID-bound deferred cleanup reminder in `PodWorkqueue`. The handler returns the
    /// deletionTimestamp-tagged object immediately; kubelet actor finalization
    /// owns the final UID-bound row removal after runtime cleanup is confirmed.
    pub async fn api_delete_pod(
        &self,
        ns: &str,
        name: &str,
        options: DeleteOptions,
        dry_run: bool,
    ) -> Result<PodApiDeleteOutcome, AppError> {
        self.api_delete_pod_inner(ns, name, options, dry_run, true)
            .await
    }

    pub async fn api_delete_pod_for_gc(
        &self,
        ns: &str,
        name: &str,
        options: DeleteOptions,
        dry_run: bool,
    ) -> Result<PodApiDeleteOutcome, AppError> {
        self.api_delete_pod_inner(ns, name, options, dry_run, false)
            .await
    }

    async fn api_delete_pod_inner(
        &self,
        ns: &str,
        name: &str,
        options: DeleteOptions,
        dry_run: bool,
        cascade_dependents: bool,
    ) -> Result<PodApiDeleteOutcome, AppError> {
        let resource = self
            .store
            .get(ns, name)
            .await?
            .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;
        let delete_preconditions = options
            .resource_preconditions()
            .map_err(AppError::BadRequest)?;
        ensure_resource_preconditions_match(&resource, &delete_preconditions)?;

        let delete_options_value = serde_json::to_value(&options).unwrap_or_else(|_| json!({}));
        let _ = run_admission_for_request(
            self.db.as_ref(),
            build_admission_context(AdmissionContextRequest {
                api_version: "v1",
                kind: "Pod",
                operation: "DELETE",
                namespace: Some(ns.to_string()),
                name: Some(name.to_string()),
                object: Value::Null,
                old_object: Some((*resource.data).clone()),
                dry_run,
                subresource: None,
                options: Some(delete_options_value),
            }),
        )
        .await?;

        let grace_period_seconds = pod_delete_grace_period_seconds(&resource.data, &options);
        let data = pod_data_with_deletion_metadata(&resource.data, grace_period_seconds);

        if dry_run {
            let mut dry = data;
            if let Some(obj) = dry.as_object_mut()
                && let Some(meta) = obj.get_mut("metadata").and_then(|m| m.as_object_mut())
            {
                meta.insert(
                    "resourceVersion".to_string(),
                    Value::String(resource.resource_version.to_string()),
                );
            }
            return Ok(PodApiDeleteOutcome::DryRun(dry));
        }

        const MAX_DELETE_CONFLICT_RETRIES: u32 = 8;
        let mut current = resource;
        let mut attempt = 0u32;
        let (updated, previous) = loop {
            let delete_base = if delete_preconditions.resource_version.is_some() {
                current.clone()
            } else {
                self.store
                    .get(ns, name)
                    .await?
                    .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?
            };
            ensure_resource_preconditions_match(&delete_base, &delete_preconditions)?;
            let grace_period_seconds = pod_delete_grace_period_seconds(&delete_base.data, &options);
            let data = pod_data_with_deletion_metadata(&delete_base.data, grace_period_seconds);
            let mark_result = if delete_preconditions.resource_version.is_some() {
                self.store
                    .mark_deleting_at_resource_version(
                        ns,
                        name,
                        &delete_base.uid,
                        data,
                        delete_base.resource_version,
                    )
                    .await
            } else {
                self.store
                    .mark_deleting_latest(ns, name, &delete_base.uid, &data)
                    .await
            };
            match mark_result {
                Ok(updated) => break (updated, std::sync::Arc::unwrap_or_clone(delete_base.data)),
                Err(e) if is_conflict_error(&e) && attempt + 1 < MAX_DELETE_CONFLICT_RETRIES => {
                    let backoff_ms = std::cmp::min(20u64.saturating_mul(1u64 << attempt), 250);
                    let _ = self
                        .supervisor
                        .sleep(
                            "pod_delete_conflict_retry_backoff",
                            Duration::from_millis(backoff_ms),
                        )
                        .await;
                    current = self
                        .store
                        .get(ns, name)
                        .await?
                        .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;
                    attempt += 1;
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        };

        let uid = updated
            .data
            .get("metadata")
            .and_then(|m| m.get("uid"))
            .and_then(|u| u.as_str())
            .unwrap_or("")
            .to_string();
        if let Err(err) = crate::side_effects::service_pod::enqueue_services_after_pod_update(
            &previous,
            &updated.data,
            self.store.db().as_ref(),
            &self.side_effects.controller_dispatcher_slot(),
        )
        .await
        {
            tracing::debug!(
                target: "klights::pod_repository::api",
                error = %err,
                "failed to enqueue Service reconcile after pod endpoint state changed"
            );
        }
        // Cascade-delete dependents (resources with ownerReferences
        // pointing to this pod). This handles the GC dependency-circle
        // conformance test where deleting pod1 must cascade to pod2
        // (owned by pod1) → pod3 (owned by pod2).
        if cascade_dependents
            && let Err(e) = crate::controllers::gc::cascade_delete_with_uid(
                self.db.as_ref(),
                &uid,
                "v1",
                name,
                "Pod",
                Some(ns.to_string()),
                self as &dyn crate::controllers::gc::GcPodDeleteSink,
            )
            .await
        {
            self.metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                namespace = %ns,
                name = %name,
                error = %e,
                "pod delete: cascade delete of dependents failed"
            );
        }

        // Fallback cleanup reminder. The kubelet-side runtime cleanup path
        // removes the API object after hooks and sandbox teardown complete.
        // The workqueue is UID-bound and must not free the name slot while the
        // same UID still exists; otherwise force/grace=0 could bypass preStop
        // and CRI cleanup or delete a same-name replacement.
        let deferred_delete_delay = Duration::from_secs(grace_period_seconds as u64);
        if let Err(e) = self
            .workqueue
            .enqueue_deferred_delete_with_target_node(
                ns.to_string(),
                name.to_string(),
                uid,
                deferred_delete_delay,
                pod_target_node_from_pod_data(&updated.data),
            )
            .await
        {
            self.metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(
                namespace = %ns,
                name = %name,
                error = %e,
                "failed to enqueue pod deferred delete"
            );
        }

        Ok(PodApiDeleteOutcome::GracefulSet(updated))
    }

    /// List all matching pods and call `api_delete_pod` for each. Mirrors
    /// today's `delete_collection_*` macro arm semantics — best-effort
    /// (errors logged, loop continues) and returns a Status:Success
    /// response at the handler.
    pub async fn api_delete_collection_pods(
        &self,
        ns: &str,
        label_selector: Option<&str>,
        field_selector: Option<&str>,
        dry_run: bool,
    ) -> Result<(), AppError> {
        // Bulk delete marks each matching Pod terminating and records the
        // UID-bound deferred actor wake. The kubelet actor owns final row
        // removal after runtime/cache cleanup; collection delete must not free
        // the namespace/name slot inline.
        let list = self
            .store
            .list(Some(ns), label_selector, field_selector, None, None)
            .await?;
        for r in list.items {
            let owner_uid = r
                .data
                .get("metadata")
                .and_then(|m| m.get("uid"))
                .and_then(|u| u.as_str())
                .unwrap_or("")
                .to_string();
            let res_name = r.name.clone();
            if let Err(e) = self
                .api_delete_pod(ns, &res_name, DeleteOptions::default(), dry_run)
                .await
            {
                self.metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    namespace = %ns,
                    name = %res_name,
                    error = ?e,
                    "delete collection: pod termination failed"
                );
                continue;
            }
            if !dry_run
                && let Err(e) = crate::controllers::gc::cascade_delete_with_uid(
                    self.db.as_ref(),
                    &owner_uid,
                    "v1",
                    &res_name,
                    "Pod",
                    Some(ns.to_string()),
                    self as &dyn crate::controllers::gc::GcPodDeleteSink,
                )
                .await
            {
                self.metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    namespace = %ns,
                    name = %res_name,
                    error = %e,
                    "delete collection: cascade delete failed"
                );
            }
        }
        Ok(())
    }
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
        .and_then(|conditions| conditions.as_array())
        .and_then(|conditions| {
            conditions.iter().find(|condition| {
                condition.get("type").and_then(|v| v.as_str()) == Some("PodScheduled")
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
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string)
}

fn initial_create_scheduling_decision(pod: &Value) -> PodSchedulingDecision {
    let explicit_node_name = pod
        .pointer("/spec/nodeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if let Some(explicit_node_name) = explicit_node_name {
        PodSchedulingDecision {
            node_name: Some(explicit_node_name.to_string()),
            pending: false,
            unschedulable_message: None,
            preemption_victims: Vec::new(),
        }
    } else {
        PodSchedulingDecision {
            node_name: None,
            pending: true,
            unschedulable_message: None,
            preemption_victims: Vec::new(),
        }
    }
}

fn sorted_unbound_pods(pods: Vec<Resource>) -> Vec<Resource> {
    let mut pods: Vec<Resource> = pods
        .into_iter()
        .filter(|pod| {
            pod.data
                .pointer("/spec/nodeName")
                .and_then(|v| v.as_str())
                .is_none_or(|s| s.is_empty())
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
        .and_then(|v| v.as_str())
        .unwrap_or("")
}

fn pod_namespace(pod: &Resource) -> String {
    pod.namespace
        .clone()
        .or_else(|| {
            pod.data
                .pointer("/metadata/namespace")
                .and_then(|v| v.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "default".to_string())
}

fn reserved_pod_body(pod: &Resource, node_name: &str) -> Value {
    let mut body = std::sync::Arc::unwrap_or_clone(pod.data.clone());
    if let Some(obj) = body.as_object_mut() {
        let spec = obj.entry("spec".to_string()).or_insert_with(|| json!({}));
        if let Some(spec) = spec.as_object_mut() {
            spec.insert("nodeName".to_string(), json!(node_name));
        }
    }
    body
}

struct SchedulerSnapshot {
    nodes: Vec<Resource>,
    pods: Vec<Resource>,
    namespaces: Vec<Resource>,
    pdbs: Vec<Resource>,
}

async fn schedule_pod_from_snapshot(
    store: &PodStore,
    snapshot: &SchedulerSnapshot,
    pod: &Value,
    namespace: &str,
    pod_name: &str,
    reservations: &[Value],
) -> Result<PodSchedulingDecision, AppError> {
    let explicit_node_name = pod
        .pointer("/spec/nodeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if let Some(explicit_node_name) = explicit_node_name {
        return Ok(PodSchedulingDecision {
            node_name: Some(explicit_node_name.to_string()),
            pending: false,
            unschedulable_message: None,
            preemption_victims: Vec::new(),
        });
    }

    let mut node_names: Vec<String> = snapshot.nodes.iter().map(|n| n.name.clone()).collect();
    node_names.sort();

    let node_values: Vec<&Value> = snapshot.nodes.iter().map(|n| n.data.as_ref()).collect();
    let namespace_values: Vec<&Value> = snapshot
        .namespaces
        .iter()
        .map(|namespace| namespace.data.as_ref())
        .collect();
    let pdb_values: Vec<&Value> = snapshot.pdbs.iter().map(|pdb| pdb.data.as_ref()).collect();
    let existing_per_node: Vec<(&str, Vec<&Value>)> = node_names
        .iter()
        .map(|name| {
            let mut pods_on_node: Vec<&Value> = snapshot
                .pods
                .iter()
                .filter(|p| pod_counts_toward_node_allocated(&p.data, name, namespace, pod_name))
                .map(|p| p.data.as_ref())
                .collect();
            pods_on_node.extend(
                reservations
                    .iter()
                    .filter(|p| pod_counts_toward_node_allocated(p, name, namespace, pod_name)),
            );
            (name.as_str(), pods_on_node)
        })
        .collect();

    let decision = crate::scheduler::engine::schedule_from_json_with_policy(
        &node_values,
        pod,
        &existing_per_node
            .iter()
            .map(|(name, pods)| (*name, pods.as_slice()))
            .collect::<Vec<_>>(),
        &namespace_values,
        &pdb_values,
    );

    let mut api_decision = scheduling_decision_to_api(decision);
    if !api_decision.preemption_victims.is_empty() {
        let victim_keys: Vec<String> = api_decision
            .preemption_victims
            .iter()
            .map(|v| format!("{}/{}", v.namespace, v.name))
            .collect();
        if let Some(node_name) = api_decision.node_name.as_deref() {
            api_decision.preemption_victims =
                collect_preemption_victims_with_data(store, node_name, pod, &victim_keys).await?;
        }
    }

    Ok(api_decision)
}

async fn schedule_pod_on_available_nodes(
    store: &PodStore,
    db: &dyn crate::datastore::DatastoreBackend,
    pod: &Value,
    namespace: &str,
    pod_name: &str,
) -> Result<PodSchedulingDecision, AppError> {
    let explicit_node_name = pod
        .pointer("/spec/nodeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if let Some(explicit_node_name) = explicit_node_name {
        return Ok(PodSchedulingDecision {
            node_name: Some(explicit_node_name.to_string()),
            pending: false,
            unschedulable_message: None,
            preemption_victims: Vec::new(),
        });
    }

    let nodes = db
        .list_resources(
            "v1",
            "Node",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let namespaces = db
        .list_resources(
            "v1",
            "Namespace",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;
    let pdbs = db
        .list_resources(
            "policy/v1",
            "PodDisruptionBudget",
            None,
            crate::datastore::ResourceListQuery::all(),
        )
        .await?;

    // Collect existing pods on each node
    let all_pods = store
        .list(None, None, None, None, None)
        .await
        .map_err(|e| -> AppError { e.into() })?;

    let mut node_names: Vec<String> = nodes.items.iter().map(|n| n.name.clone()).collect();
    node_names.sort();

    let node_values: Vec<&Value> = nodes.items.iter().map(|n| n.data.as_ref()).collect();
    let namespace_values: Vec<&Value> = namespaces
        .items
        .iter()
        .map(|namespace| namespace.data.as_ref())
        .collect();
    let pdb_values: Vec<&Value> = pdbs.items.iter().map(|pdb| pdb.data.as_ref()).collect();
    let existing_per_node: Vec<(&str, Vec<&Value>)> = node_names
        .iter()
        .map(|name| {
            let pods_on_node: Vec<&Value> = all_pods
                .items
                .iter()
                .filter(|p| pod_counts_toward_node_allocated(&p.data, name, namespace, pod_name))
                .map(|p| p.data.as_ref())
                .collect();
            (name.as_str(), pods_on_node)
        })
        .collect();

    let decision = crate::scheduler::engine::schedule_from_json_with_policy(
        &node_values,
        pod,
        &existing_per_node
            .iter()
            .map(|(name, pods)| (*name, pods.as_slice()))
            .collect::<Vec<_>>(),
        &namespace_values,
        &pdb_values,
    );

    let mut api_decision = scheduling_decision_to_api(decision);
    if !api_decision.preemption_victims.is_empty() {
        let victim_keys: Vec<String> = api_decision
            .preemption_victims
            .iter()
            .map(|v| format!("{}/{}", v.namespace, v.name))
            .collect();
        if let Some(node_name) = api_decision.node_name.as_deref() {
            api_decision.preemption_victims =
                collect_preemption_victims_with_data(store, node_name, pod, &victim_keys).await?;
        }
    }

    Ok(api_decision)
}

fn scheduling_decision_to_api(
    decision: crate::scheduler::types::SchedulingDecision,
) -> PodSchedulingDecision {
    PodSchedulingDecision {
        node_name: decision.selected_node,
        pending: false,
        unschedulable_message: decision.unschedulable_message,
        preemption_victims: decision
            .preemption_victims
            .iter()
            .map(|v| {
                let parts: Vec<&str> = v.splitn(2, '/').collect();
                PreemptionVictim {
                    namespace: parts.first().unwrap_or(&"").to_string(),
                    name: parts.get(1).unwrap_or(&"").to_string(),
                    resource_version: 0,
                    data: Value::Null,
                }
            })
            .collect(),
    }
}

async fn collect_preemption_victims_with_data(
    store: &PodStore,
    node_name: &str,
    incoming: &Value,
    victim_names: &[String],
) -> Result<Vec<PreemptionVictim>, AppError> {
    let incoming_priority = pod_priority(incoming);
    let pods = store
        .list(None, None, None, None, None)
        .await
        .map_err(|e| -> AppError { e.into() })?;
    let mut victims = Vec::new();
    for resource in pods.items {
        if !pod_counts_toward_node_allocated(&resource.data, node_name, "", "") {
            continue;
        }
        if pod_priority(&resource.data) >= incoming_priority {
            continue;
        }
        let ns = resource
            .namespace
            .clone()
            .or_else(|| {
                resource
                    .data
                    .pointer("/metadata/namespace")
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
            })
            .unwrap_or_else(|| "default".to_string());
        let key = format!("{}/{}", ns, resource.name);
        if victim_names.contains(&key) {
            victims.push(PreemptionVictim {
                namespace: ns,
                name: resource.name,
                resource_version: resource.resource_version,
                data: std::sync::Arc::unwrap_or_clone(resource.data),
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
        .and_then(|v| v.as_str())
        .is_some()
    {
        return false;
    }
    if pod.pointer("/spec/nodeName").and_then(|v| v.as_str()) != Some(node_name) {
        return false;
    }
    let namespace = pod
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("default");
    let name = pod.pointer("/metadata/name").and_then(|v| v.as_str());
    if namespace == pending_namespace && name == Some(pending_name) {
        return false;
    }
    !matches!(
        pod.pointer("/status/phase").and_then(|v| v.as_str()),
        Some("Succeeded" | "Failed")
    )
}

async fn apply_preemption_victims(
    ctx: PreemptionApplyContext<'_>,
    victims: &[PreemptionVictim],
) -> Result<(), AppError> {
    for victim in victims {
        let updated = write_preemption_termination(
            ctx.store,
            victim,
            ctx.preemptor_namespace,
            ctx.preemptor_name,
        )
        .await?;
        let uid = updated.uid.clone();
        let hook_resource = std::sync::Arc::unwrap_or_clone(updated.data);
        ctx.workqueue
            .enqueue_deferred_delete_with_target_node(
                victim.namespace.clone(),
                victim.name.clone(),
                uid,
                Duration::ZERO,
                pod_target_node_from_pod_data(&hook_resource),
            )
            .await
            .map_err(|e| -> AppError { e.into() })?;
        crate::side_effects::run_hooks_logged(
            ctx.side_effects,
            &hook_resource,
            ctx.store.db().as_ref(),
            ctx.metrics,
            "pod_preemption_victim",
        )
        .await;
    }
    Ok(())
}

async fn write_preemption_termination(
    store: &PodStore,
    victim: &PreemptionVictim,
    preemptor_namespace: &str,
    preemptor_name: &str,
) -> Result<Resource, AppError> {
    const MAX_RETRIES: u32 = 5;
    let mut resource_version = victim.resource_version;
    let mut data = victim.data.clone();

    for attempt in 0..MAX_RETRIES {
        mark_pod_preempted_metadata(&mut data);
        let mut status = preempted_status(&data, preemptor_namespace, preemptor_name);
        // This is a scheduler-originated write (preemption sets the
        // scheduler-owned `DisruptionTarget` condition). Route it through the
        // central Pod status merge policy as the Scheduler owner so it is
        // authoritative for scheduler conditions while kubelet-owned lifecycle
        // conditions present in the live status are preserved, and no terminal
        // container-state rewrite is applied (that is a kubelet-only guarantee).
        crate::pod_status_merge::merge_pod_status_for_update(
            "v1",
            "Pod",
            &data,
            &mut status,
            crate::pod_status_merge::PodStatusOwner::Scheduler,
        );
        if let Some(object) = data.as_object_mut() {
            object.insert("status".to_string(), status);
        }
        match store
            .update_including_status_for_scheduler(
                &victim.namespace,
                &victim.name,
                data,
                resource_version,
            )
            .await
        {
            Ok(updated) => return Ok(updated),
            Err(e) if attempt + 1 < MAX_RETRIES && is_conflict_error(&e) => {
                let current = store
                    .get(&victim.namespace, &victim.name)
                    .await
                    .map_err(|e| -> AppError { e.into() })?
                    .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;
                resource_version = current.resource_version;
                data = std::sync::Arc::unwrap_or_clone(current.data);
            }
            Err(e) => return Err(e.into()),
        }
    }

    unreachable!("preemption termination retry loop exhausted without returning")
}

fn mark_pod_preempted_metadata(data: &mut Value) {
    let now = crate::utils::k8s_timestamp();
    if let Some(meta) = data.get_mut("metadata").and_then(|v| v.as_object_mut()) {
        meta.entry("deletionTimestamp".to_string())
            .or_insert_with(|| json!(now.clone()));
        meta.entry("deletionGracePeriodSeconds".to_string())
            .or_insert_with(|| json!(0));
    }
}

fn preempted_status(data: &Value, preemptor_namespace: &str, preemptor_name: &str) -> Value {
    let mut status = data.get("status").cloned().unwrap_or_else(|| json!({}));
    if !status.is_object() {
        status = json!({});
    }
    let condition = json!({
        "type": "DisruptionTarget",
        "status": "True",
        "lastTransitionTime": crate::utils::k8s_timestamp(),
        "reason": "PreemptionByScheduler",
        "message": format!("Preempted by pod {preemptor_namespace}/{preemptor_name} on node")
    });
    if let Some(status) = status.as_object_mut() {
        let conditions = status
            .entry("conditions".to_string())
            .or_insert_with(|| json!([]));
        if let Some(conditions) = conditions.as_array_mut() {
            conditions.retain(|existing| {
                existing.get("type").and_then(|v| v.as_str()) != Some("DisruptionTarget")
            });
            conditions.push(condition);
        }
    }
    status
}

fn validate_pod_binding_object(
    namespace: &str,
    name: &str,
    binding: &Value,
) -> Result<(), AppError> {
    if binding.get("kind").and_then(|v| v.as_str()) != Some("Binding") {
        return Err(AppError::BadRequest(
            "Binding.kind must be \"Binding\"".to_string(),
        ));
    }
    if binding.get("apiVersion").and_then(|v| v.as_str()) != Some("v1") {
        return Err(AppError::BadRequest(
            "Binding.apiVersion must be \"v1\"".to_string(),
        ));
    }
    let metadata_name = binding
        .pointer("/metadata/name")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("Binding.metadata.name is required".to_string()))?;
    if metadata_name != name {
        return Err(AppError::BadRequest(format!(
            "Binding.metadata.name must match URL pod name \"{name}\""
        )));
    }
    if let Some(metadata_namespace) = binding
        .pointer("/metadata/namespace")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        && metadata_namespace != namespace
    {
        return Err(AppError::BadRequest(format!(
            "Binding.metadata.namespace must match URL namespace \"{namespace}\""
        )));
    }
    if binding.pointer("/target/kind").and_then(|v| v.as_str()) != Some("Node") {
        return Err(AppError::BadRequest(
            "Binding.target.kind must be \"Node\"".to_string(),
        ));
    }
    if let Some(target_api_version) = binding
        .pointer("/target/apiVersion")
        .and_then(|v| v.as_str())
        && !target_api_version.is_empty()
        && target_api_version != "v1"
    {
        return Err(AppError::BadRequest(
            "Binding.target.apiVersion must be \"v1\"".to_string(),
        ));
    }
    let target_name = binding
        .pointer("/target/name")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| AppError::BadRequest("Binding.target.name is required".to_string()))?;
    if !validate_dns_subdomain(target_name) {
        return Err(AppError::BadRequest(format!(
            "Binding.target.name \"{target_name}\" is not a valid node name"
        )));
    }
    Ok(())
}

fn binding_resource_preconditions(binding: &Value) -> Result<ResourcePreconditions, AppError> {
    let uid = binding
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let resource_version = match binding.pointer("/metadata/resourceVersion") {
        None | Some(Value::Null) => None,
        Some(Value::String(raw)) if raw.is_empty() => None,
        Some(Value::String(raw)) => Some(raw.parse::<i64>().map_err(|_| {
            AppError::BadRequest(format!(
                "Invalid value: \"{raw}\": resourceVersion must be an integer"
            ))
        })?),
        Some(Value::Number(number)) => Some(number.as_i64().ok_or_else(|| {
            AppError::BadRequest(
                "Invalid value: metadata.resourceVersion must be an integer".to_string(),
            )
        })?),
        Some(_) => {
            return Err(AppError::BadRequest(
                "Invalid value: metadata.resourceVersion must be a string".to_string(),
            ));
        }
    };
    Ok(ResourcePreconditions {
        uid,
        resource_version,
    })
}

fn merge_binding_annotations(pod: &mut Value, binding: &Value) {
    let Some(binding_annotations) = binding
        .pointer("/metadata/annotations")
        .and_then(|v| v.as_object())
    else {
        return;
    };
    if binding_annotations.is_empty() {
        return;
    }
    let Some(pod_object) = pod.as_object_mut() else {
        return;
    };
    let metadata = pod_object
        .entry("metadata".to_string())
        .or_insert_with(|| json!({}));
    let Some(metadata_object) = metadata.as_object_mut() else {
        return;
    };
    let annotations = metadata_object
        .entry("annotations".to_string())
        .or_insert_with(|| json!({}));
    let Some(annotations_object) = annotations.as_object_mut() else {
        return;
    };
    for (key, value) in binding_annotations {
        annotations_object.insert(key.clone(), value.clone());
    }
}

fn set_bound_node_name(pod: &mut Value, node_name: &str) -> Result<(), AppError> {
    let pod_object = pod
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("Pod body must be an object".to_string()))?;
    let spec = pod_object
        .entry("spec".to_string())
        .or_insert_with(|| json!({}));
    let spec_object = spec
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("Pod.spec must be an object".to_string()))?;
    spec_object.insert("nodeName".to_string(), json!(node_name));
    Ok(())
}

fn upsert_pod_scheduled_true(pod: &mut Value) -> Result<(), AppError> {
    let pod_object = pod
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("Pod body must be an object".to_string()))?;
    let status = pod_object
        .entry("status".to_string())
        .or_insert_with(|| json!({}));
    let status_object = status
        .as_object_mut()
        .ok_or_else(|| AppError::BadRequest("Pod.status must be an object".to_string()))?;
    status_object.remove("nominatedNodeName");
    let conditions = status_object
        .entry("conditions".to_string())
        .or_insert_with(|| json!([]));
    let conditions_array = conditions.as_array_mut().ok_or_else(|| {
        AppError::BadRequest("Pod.status.conditions must be an array".to_string())
    })?;
    conditions_array
        .retain(|condition| condition.get("type").and_then(|v| v.as_str()) != Some("PodScheduled"));
    conditions_array.push(json!({
        "type": "PodScheduled",
        "status": "True",
        "lastTransitionTime": crate::utils::k8s_timestamp()
    }));
    Ok(())
}

fn pod_data_with_deletion_metadata(data: &Value, grace_period_seconds: i64) -> Value {
    let mut data = data.clone();
    if let Some(meta) = data.get_mut("metadata").and_then(|m| m.as_object_mut())
        && meta
            .get("deletionTimestamp")
            .is_none_or(|timestamp| timestamp.is_null())
    {
        meta.insert(
            "deletionTimestamp".to_string(),
            Value::String(crate::utils::k8s_timestamp()),
        );
        meta.insert(
            "deletionGracePeriodSeconds".to_string(),
            json!(grace_period_seconds),
        );
    }
    if data
        .pointer("/metadata/deletionTimestamp")
        .is_some_and(|timestamp| !timestamp.is_null())
    {
        crate::resource_semantics::mark_terminating_pod_unready(&mut data);
    }
    data
}

fn pod_delete_grace_period_seconds(data: &Value, options: &DeleteOptions) -> i64 {
    options
        ._grace_period_seconds
        .or_else(|| {
            data.pointer("/spec/terminationGracePeriodSeconds")
                .and_then(|value| value.as_i64())
        })
        .unwrap_or(30)
        .max(0)
}

fn is_conflict_error(err: &anyhow::Error) -> bool {
    crate::datastore::errors::is_conflict_error(err)
}

fn pod_target_node_from_pod_data(pod: &Value) -> Option<String> {
    pod.pointer("/spec/nodeName")
        .and_then(|node| node.as_str())
        .filter(|node| !node.trim().is_empty())
        .map(ToString::to_string)
}

async fn apply_priority_class_to_pod(
    db: &dyn crate::datastore::DatastoreBackend,
    pod: &mut Value,
) -> Result<(), AppError> {
    let class_name = pod
        .pointer("/spec/priorityClassName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    let (priority, policy) = resolve_priority_class(db, class_name.as_deref()).await?;
    let Some(priority) = priority else {
        return Ok(());
    };
    let spec = pod
        .as_object_mut()
        .map(|obj| obj.entry("spec".to_string()).or_insert_with(|| json!({})));
    if let Some(spec) = spec.and_then(|v| v.as_object_mut()) {
        spec.insert("priority".to_string(), json!(priority));
        if !spec.contains_key("preemptionPolicy")
            && let Some(policy) = policy
        {
            spec.insert("preemptionPolicy".to_string(), json!(policy));
        }
    }
    Ok(())
}

async fn resolve_priority_class(
    db: &dyn crate::datastore::DatastoreBackend,
    class_name: Option<&str>,
) -> Result<(Option<i64>, Option<String>), AppError> {
    match class_name {
        Some("system-node-critical") => Ok((Some(2_000_001_000_i64), None)),
        Some("system-cluster-critical") => Ok((Some(2_000_000_000_i64), None)),
        Some(class_name) => {
            let pc = db
                .get_resource("scheduling.k8s.io/v1", "PriorityClass", None, class_name)
                .await?;
            Ok(priority_class_value_and_policy(
                pc.as_ref().map(|pc| pc.data.as_ref()),
            ))
        }
        None => {
            let classes = db
                .list_resources(
                    "scheduling.k8s.io/v1",
                    "PriorityClass",
                    None,
                    crate::datastore::ResourceListQuery::all(),
                )
                .await?;
            let default_class = classes.items.iter().find(|pc| {
                pc.data
                    .get("globalDefault")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            });
            Ok(priority_class_value_and_policy(
                default_class.map(|pc| pc.data.as_ref()),
            ))
        }
    }
}

fn priority_class_value_and_policy(pc: Option<&Value>) -> (Option<i64>, Option<String>) {
    let priority = pc.and_then(|pc| pc.get("value")).and_then(|v| v.as_i64());
    let policy = pc
        .and_then(|pc| pc.get("preemptionPolicy"))
        .and_then(|v| v.as_str())
        .map(ToString::to_string);
    (priority, policy)
}

fn pod_priority(pod: &Value) -> i64 {
    pod.pointer("/spec/priority")
        .and_then(|v| v.as_i64())
        .unwrap_or(0)
}

fn patch_type_to_content_type(p: PodStatusPatchType) -> &'static str {
    match p {
        PodStatusPatchType::JsonPatch => "application/json-patch+json",
        PodStatusPatchType::MergePatch => "application/merge-patch+json",
        PodStatusPatchType::StrategicMerge => "application/strategic-merge-patch+json",
        PodStatusPatchType::ApplyPatch => "application/apply-patch+yaml",
    }
}

#[async_trait::async_trait]
impl crate::controllers::gc::GcPodDeleteSink for PodApiService {
    async fn request_gc_pod_delete(
        &self,
        namespace: &str,
        name: &str,
        uid: &str,
    ) -> anyhow::Result<()> {
        let options = crate::api::DeleteOptions::with_uid_precondition(uid);
        match self
            .api_delete_pod_for_gc(namespace, name, options, false)
            .await
        {
            Ok(_outcome) => Ok(()),
            Err(e) => Err(anyhow::anyhow!("{e:?}")),
        }
    }
}
