//! Neutral Pod API and scheduling orchestration shared by focused services.
//! pipeline (today the body of `src/pod_create.rs::create_pod_through_pipeline`
//! and the Pod-specific arms of `namespaced_resource_handlers!`).
//!
//! Persistence, deletion, admission reads, placement, event emission, and
//! reconciliation arrive through focused neutral capabilities. Concrete
//! kubelet repositories, datastores, scheduler engines, and event writers
//! remain behind root-owned adapters. The orchestration owner never schedules
//! an unsupervised task or reaches those implementations directly.
//!
//! Implementations land in Tasks 11 (create), 12 (update/patch), and 13
//! (delete + delete-collection).

use std::sync::Arc;
use std::time::Duration;

use serde_json::{Value, json};

use crate::api::{
    AdmissionContextRequest, AdmissionResourceStore, apply_limitrange_defaults_to_pod, apply_patch,
    apply_pod_runtimeclass_admission, apply_pod_service_account_defaults,
    apply_pod_spec_create_defaults, build_admission_context, check_resource_quota_for_creation,
    check_resource_quota_for_pod_update, compute_qos_class, enforce_limitrange_constraints_for_pod,
    enforce_pod_security_admission, normalize_resource_for_storage, resolve_resource_name,
    run_admission_for_request, validate_builtin_resource_spec, validate_dns_subdomain,
    validate_pod_resource_requirements_immutable, validate_pod_sysctls,
};
use crate::api::{AppError, DeleteOptions};
use klights_cluster_core::{Resource, ResourcePreconditions};
use klights_pod_api::{
    PodActorFinalizeRequest, PodApiCreateResult, PodApiDeleteOutcome, PodApiWriteOutcome,
    PodDeleteMarkRequest, PodDeleteOptions, PodDeleteOrchestration, PodDeletePreconditions,
    PodGetRequest, PodListRequest, PodMutationTarget, PodPersistence, PodPersistenceCreateRequest,
    PodPersistenceReplaceRequest, PodQuery, PodRepositoryError, PodSpecValidation,
    PodStatusPatchKind, preserve_pod_status_from_current,
};
use klights_reconcile_api::{
    GcPodDeleteError, GcPodDeleteFuture, GcPodDeleteRequest, GcPodDeleteSink, PodGcReconcileSink,
    PodServiceReconcileSink, ReconcileFailureMetrics,
};
use klights_supervisor::TaskSupervisor;
use klights_types::PodIdentity;

type PodApiUpdateOutcome = PodApiWriteOutcome;
type PodStatusPatchType = PodStatusPatchKind;

struct PodApiCreateRequest {
    namespace: String,
    name: String,
    body: Value,
    dry_run: bool,
    run_admission: bool,
}

impl From<DeleteOptions> for PodDeleteOptions {
    fn from(options: DeleteOptions) -> Self {
        let preconditions = options.preconditions.unwrap_or_default();
        Self::new(
            options.propagation_policy,
            options.orphan_dependents,
            options._grace_period_seconds,
            PodDeletePreconditions::new(preconditions.uid, preconditions.resource_version),
        )
    }
}

fn ensure_resource_preconditions_match(
    resource: &Resource,
    preconditions: &ResourcePreconditions,
) -> Result<(), AppError> {
    crate::resource_preconditions::ensure_delete_preconditions_match(resource, preconditions)
        .map_err(AppError::from)
}

fn pod_get_request(namespace: &str, name: &str) -> Result<PodGetRequest, AppError> {
    PodGetRequest::try_by_name(namespace.to_string(), name.to_string()).map_err(AppError::from)
}

fn pod_list_request(
    namespace: Option<&str>,
    label_selector: Option<&str>,
    field_selector: Option<&str>,
) -> Result<PodListRequest, AppError> {
    PodListRequest::try_new(
        namespace.map(ToString::to_string),
        label_selector.map(ToString::to_string),
        field_selector.map(ToString::to_string),
        None,
        None,
    )
    .map_err(AppError::from)
}

async fn apply_pod_service_account_admission(
    resources: &(impl AdmissionResourceStore + ?Sized),
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
    let Some(service_account) = resources
        .get_admission_resource(
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
struct InitialPodSchedulingState {
    node_name: Option<String>,
    pending: bool,
}

pub struct PodNativeOrchestration {
    pod_query: Arc<dyn PodQuery>,
    persistence: Arc<dyn PodPersistence>,
    deletion: Arc<dyn PodDeleteOrchestration>,
    admission_resources: Arc<dyn AdmissionResourceStore>,
    spec_validation: Arc<dyn PodSpecValidation>,
    admission: Arc<dyn crate::api::admission_ports::ResourceAdmissionPort>,
    resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    quota_runtime: Arc<dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime>,
    supervisor: Arc<TaskSupervisor>,
    gc_reconcile: Arc<dyn PodGcReconcileSink>,
    service_reconcile: Arc<dyn PodServiceReconcileSink>,
    metrics: Arc<dyn ReconcileFailureMetrics>,
    wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

pub struct PodNativeOrchestrationDependencies {
    pub pod_query: Arc<dyn PodQuery>,
    pub persistence: Arc<dyn PodPersistence>,
    pub deletion: Arc<dyn PodDeleteOrchestration>,
    pub admission_resources: Arc<dyn AdmissionResourceStore>,
    pub spec_validation: Arc<dyn PodSpecValidation>,
    pub admission: Arc<dyn crate::api::admission_ports::ResourceAdmissionPort>,
    pub resource_query: Arc<dyn klights_leader_api::LeaderResourceQuery>,
    pub quota_runtime: Arc<dyn klights_reconcile_api::ResourceQuotaAdmissionRuntime>,
    pub supervisor: Arc<TaskSupervisor>,
    pub gc_reconcile: Arc<dyn PodGcReconcileSink>,
    pub service_reconcile: Arc<dyn PodServiceReconcileSink>,
    pub metrics: Arc<dyn ReconcileFailureMetrics>,
    pub wall_clock: Arc<dyn klights_supervisor::WallClock>,
}

impl PodNativeOrchestration {
    pub fn new(dependencies: PodNativeOrchestrationDependencies) -> Self {
        let PodNativeOrchestrationDependencies {
            pod_query,
            persistence,
            deletion,
            admission_resources,
            spec_validation,
            admission,
            resource_query,
            quota_runtime,
            supervisor,
            gc_reconcile,
            service_reconcile,
            metrics,
            wall_clock,
        } = dependencies;
        Self {
            pod_query,
            persistence,
            deletion,
            admission_resources,
            spec_validation,
            admission,
            resource_query,
            quota_runtime,
            supervisor,
            gc_reconcile,
            service_reconcile,
            metrics,
            wall_clock,
        }
    }

    /// Body of `src/pod_create.rs::create_pod_through_pipeline`, moved
    /// into the repository. The single `("v1","Pod",...)` DB call is the
    /// final `store.create(...)` — every other DB touch is admission,
    /// quota, or limit-range helpers against other kinds, which legitimately
    /// flow through `self.db`.
    async fn api_create_pod(
        &self,
        request: PodApiCreateRequest,
    ) -> Result<PodApiCreateResult, AppError> {
        let operation_now = self.wall_clock.now_utc();
        let creation_time = klights_cluster_core::k8s_time::format_time(operation_now.to_owned());
        let transition_time =
            klights_cluster_core::k8s_time::format_legacy_timestamp(operation_now);
        let PodApiCreateRequest {
            namespace,
            name,
            mut body,
            dry_run,
            run_admission,
        } = request;

        let namespace_resource = self
            .admission_resources
            .get_admission_resource("v1", "Namespace", None, &namespace)
            .await?;
        match crate::namespace_admission::classify_namespace(
            &namespace,
            namespace_resource
                .as_ref()
                .map(|resource| resource.data.as_ref()),
        ) {
            crate::namespace_admission::NamespaceCreateEligibility::Allowed => {}
            crate::namespace_admission::NamespaceCreateEligibility::Missing => {
                return Err(AppError::Forbidden(format!(
                    "namespace {namespace} not found"
                )));
            }
            crate::namespace_admission::NamespaceCreateEligibility::Terminating => {
                return Err(AppError::Forbidden(format!(
                    "namespace {namespace} is being terminated"
                )));
            }
        }

        self.spec_validation.validate_volume_paths(&body)?;
        validate_pod_sysctls(&body)?;

        if run_admission {
            body = run_admission_for_request(
                self.admission.as_ref(),
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

        apply_pod_runtimeclass_admission(self.admission_resources.as_ref(), &mut body).await?;
        apply_limitrange_defaults_to_pod(self.admission_resources.as_ref(), &namespace, &mut body)
            .await?;
        enforce_limitrange_constraints_for_pod(
            self.admission_resources.as_ref(),
            &namespace,
            &body,
        )
        .await?;
        validate_builtin_resource_spec("Pod", &body)?;
        apply_pod_service_account_admission(
            self.admission_resources.as_ref(),
            &namespace,
            &mut body,
        )
        .await?;

        if dry_run {
            return Ok(PodApiCreateResult {
                resource: None,
                body,
            });
        }

        check_resource_quota_for_creation(self.quota_runtime.as_ref(), &namespace, "Pod", &body)
            .await?;

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
                    Value::String(creation_time),
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

        apply_priority_class_to_pod(self.admission_resources.as_ref(), &mut body).await?;

        let qos_class = compute_qos_class(&body);
        let scheduling_state = initial_create_scheduling_state(&body);
        if let Some(obj) = body.as_object_mut() {
            let spec = obj.entry("spec".to_string()).or_insert_with(|| json!({}));
            if let Some(spec_obj) = spec.as_object_mut() {
                apply_pod_spec_create_defaults(spec_obj);
                let needs_node_name = spec_obj
                    .get("nodeName")
                    .map(|v| v.as_str().map(|s| s.is_empty()).unwrap_or(v.is_null()))
                    .unwrap_or(true);
                if needs_node_name {
                    if let Some(scheduled_node) = scheduling_state.node_name.as_deref() {
                        spec_obj.insert("nodeName".to_string(), json!(scheduled_node));
                    } else {
                        spec_obj.remove("nodeName");
                    }
                }
            }
            let pod_scheduled_condition = if scheduling_state.pending {
                json!({
                    "type": "PodScheduled",
                    "status": "False",
                    "lastTransitionTime": transition_time.clone(),
                    "reason": "SchedulingPending",
                })
            } else {
                json!({
                    "type": "PodScheduled",
                    "status": "True",
                    "lastTransitionTime": transition_time.clone(),
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
                            "lastTransitionTime": transition_time.clone(),
                        },
                        {
                            "type": "Ready",
                            "status": "False",
                            "lastTransitionTime": transition_time.clone(),
                        },
                        {
                            "type": "ContainersReady",
                            "status": "False",
                            "lastTransitionTime": transition_time.clone(),
                        },
                        pod_scheduled_condition
                    ],
                    "containerStatuses": [],
                    "qosClass": qos_class,
                }),
            );
        }

        normalize_resource_for_storage("v1", "Pod", &mut body);
        enforce_pod_security_admission(self.resource_query.as_ref(), &namespace, &body).await?;
        let resource = self
            .persistence
            .create_pod(PodPersistenceCreateRequest {
                namespace: namespace.clone(),
                name: resource_name.clone(),
                body,
            })
            .await?;
        if let Err(e) = self
            .gc_reconcile
            .reconcile_owner_references(resource.clone(), self as &dyn GcPodDeleteSink)
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
        if let Err(err) = self
            .service_reconcile
            .enqueue_after_pod_create(resource.clone())
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

    pub async fn bind_pod_from_api(
        &self,
        namespace: &str,
        name: &str,
        binding: Value,
        dry_run: bool,
    ) -> Result<(), AppError> {
        let transition_time =
            klights_cluster_core::k8s_time::format_legacy_timestamp(self.wall_clock.now_utc());
        validate_pod_binding_object(namespace, name, &binding)?;
        let target_node = binding
            .pointer("/target/name")
            .and_then(|v| v.as_str())
            .expect("validate_pod_binding_object requires target.name")
            .to_string();

        let current = self
            .pod_query
            .get_pod(pod_get_request(namespace, name)?)
            .await?
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
        upsert_pod_scheduled_true(&mut body, &transition_time)?;

        if dry_run {
            return Ok(());
        }

        self.persistence
            .replace_pod_including_status(PodPersistenceReplaceRequest {
                namespace: namespace.to_string(),
                name: name.to_string(),
                body,
                expected_resource_version: current.resource_version,
            })
            .await?;
        Ok(())
    }

    /// Body of the macro's Pod-update branch (today inlined into
    /// `pod_handlers::update_pod`). Runs Pod-specific validation,
    /// admission, immutability + quota checks, normalization, then
    /// persists via `store.update(...)` with CAS. The handler keeps the
    /// post-update side-effect calls (`maybe_hard_delete_pod_after_finalizers_drained`,
    /// `reconcile_owner_refs_after_mutation`, `state.controller_reconcile().side_effects.run_hooks`).
    pub async fn api_update_pod(
        &self,
        ns: &str,
        name: &str,
        mut body: Value,
        current: Resource,
        dry_run: bool,
    ) -> Result<PodApiUpdateOutcome, AppError> {
        self.spec_validation.validate_volume_paths(&body)?;
        validate_pod_sysctls(&body)?;

        body = run_admission_for_request(
            self.admission.as_ref(),
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
        check_resource_quota_for_pod_update(self.quota_runtime.as_ref(), ns, &current.data, &body)
            .await?;
        validate_builtin_resource_spec("Pod", &body)?;

        normalize_resource_for_storage("v1", "Pod", &mut body);
        preserve_pod_status_from_current(&current.data, &mut body);
        enforce_pod_security_admission(self.resource_query.as_ref(), ns, &body).await?;
        let requested_resource_version = body
            .pointer("/metadata/resourceVersion")
            .and_then(Value::as_str)
            .and_then(|resource_version| resource_version.parse::<i64>().ok())
            .unwrap_or(current.resource_version);

        if dry_run {
            return Ok(PodApiUpdateOutcome::DryRun(body));
        }

        let resource = self
            .persistence
            .replace_pod(PodPersistenceReplaceRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                body,
                expected_resource_version: requested_resource_version,
            })
            .await?;
        if let Err(err) = self
            .service_reconcile
            .enqueue_after_pod_update(current, resource.clone())
            .await
        {
            tracing::debug!(
                target: "klights::pod_repository::api",
                error = %err,
                "failed to enqueue Service reconcile after pod endpoint state changed"
            );
        }
        self.deletion
            .enqueue_actor_finalize_if_ready(PodActorFinalizeRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                resource: resource.clone(),
            })
            .await?;
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
            let exists = self
                .pod_query
                .get_pod(pod_get_request(ns, name)?)
                .await?
                .is_some();
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
                .pod_query
                .get_pod(pod_get_request(ns, name)?)
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
                self.admission.as_ref(),
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
            check_resource_quota_for_pod_update(
                self.quota_runtime.as_ref(),
                ns,
                &current.data,
                &patched,
            )
            .await?;
            validate_builtin_resource_spec("Pod", &patched)?;

            normalize_resource_for_storage("v1", "Pod", &mut patched);
            preserve_pod_status_from_current(&current.data, &mut patched);
            enforce_pod_security_admission(self.resource_query.as_ref(), ns, &patched).await?;

            if dry_run {
                return Ok(PodApiUpdateOutcome::DryRun(patched));
            }

            match self
                .persistence
                .replace_pod(PodPersistenceReplaceRequest {
                    namespace: ns.to_string(),
                    name: name.to_string(),
                    body: patched,
                    expected_resource_version: current.resource_version,
                })
                .await
            {
                Ok(resource) => {
                    if let Err(err) = self
                        .service_reconcile
                        .enqueue_after_pod_update(current, resource.clone())
                        .await
                    {
                        tracing::debug!(
                            target: "klights::pod_repository::api",
                            error = %err,
                            "failed to enqueue Service reconcile after pod endpoint state changed"
                        );
                    }
                    self.deletion
                        .enqueue_actor_finalize_if_ready(PodActorFinalizeRequest {
                            namespace: ns.to_string(),
                            name: name.to_string(),
                            resource: resource.clone(),
                        })
                        .await?;
                    return Ok(PodApiUpdateOutcome::Persisted(resource));
                }
                Err(e)
                    if attempt + 1 < max_retries
                        && matches!(e, PodRepositoryError::Conflict { .. }) =>
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

    /// Pod-domain adapter used by ordinary repository consumers. API
    /// DeleteOptions and HTTP errors remain inside this API-facing owner.
    pub(crate) async fn mark_pod_terminating_for_repository(
        &self,
        target: &PodMutationTarget,
    ) -> Result<Resource, PodRepositoryError> {
        let options = target
            .uid()
            .map(DeleteOptions::with_uid_precondition)
            .unwrap_or_default();
        let outcome = self
            .api_delete_pod(target.namespace(), target.name(), options, false)
            .await
            .map_err(|error| {
                map_api_error_to_pod_repository(error, target.namespace(), target.name())
            })?;
        let PodApiDeleteOutcome::GracefulSet(resource) = outcome else {
            return Err(PodRepositoryError::corrupt_response(
                "persisted graceful mark unexpectedly returned a dry-run body",
            ));
        };
        Ok(resource)
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

    pub(crate) async fn repository_delete_pod(
        &self,
        ns: &str,
        name: &str,
        options: PodDeleteOptions,
        dry_run: bool,
    ) -> Result<PodApiDeleteOutcome, PodRepositoryError> {
        let (propagation_policy, orphan_dependents, grace_period_seconds, preconditions) =
            options.into_parts();
        let (uid, resource_version) = preconditions.into_parts();
        let preconditions = if uid.is_some() || resource_version.is_some() {
            json!({
                "uid": uid,
                "resourceVersion": resource_version,
            })
        } else {
            Value::Null
        };
        let options = serde_json::from_value(json!({
            "propagationPolicy": propagation_policy,
            "orphanDependents": orphan_dependents,
            "gracePeriodSeconds": grace_period_seconds,
            "preconditions": preconditions,
        }))
        .map_err(|error| PodRepositoryError::invalid_request("deleteOptions", error.to_string()))?;
        self.api_delete_pod(ns, name, options, dry_run)
            .await
            .map_err(|error| map_api_error_to_pod_repository(error, ns, name))
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
            .pod_query
            .get_pod(pod_get_request(ns, name)?)
            .await?
            .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;
        let delete_preconditions = options
            .resource_preconditions()
            .map_err(AppError::BadRequest)?;
        ensure_resource_preconditions_match(&resource, &delete_preconditions)?;

        let delete_options_value = serde_json::to_value(&options).unwrap_or_else(|_| json!({}));
        let _ = run_admission_for_request(
            self.admission.as_ref(),
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

        if dry_run {
            return Ok(PodApiDeleteOutcome::DryRun(
                self.deletion
                    .preview_delete(&resource, options._grace_period_seconds),
            ));
        }

        let delete_outcome = self
            .deletion
            .mark_and_queue_delete(PodDeleteMarkRequest {
                namespace: ns.to_string(),
                name: name.to_string(),
                requested_grace_period_seconds: options._grace_period_seconds,
                preconditions: delete_preconditions,
                initial_resource: resource,
            })
            .await?;
        let updated = delete_outcome.updated;
        let previous = delete_outcome.previous;
        let uid = delete_outcome.uid;
        if let Err(err) = self
            .service_reconcile
            .enqueue_after_pod_update(previous, updated.clone())
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
            && let Err(e) = self
                .gc_reconcile
                .cascade_delete_dependents(
                    PodIdentity::new(ns, name, &uid),
                    self as &dyn GcPodDeleteSink,
                )
                .await
        {
            self.metrics.record_cascade_delete_failure();
            tracing::error!(
                namespace = %ns,
                name = %name,
                error = %e,
                "pod delete: cascade delete of dependents failed"
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
            .pod_query
            .list_pods(pod_list_request(Some(ns), label_selector, field_selector)?)
            .await?;
        for r in list.into_parts().0 {
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
                self.metrics.record_cascade_delete_failure();
                tracing::error!(
                    namespace = %ns,
                    name = %res_name,
                    error = ?e,
                    "delete collection: pod termination failed"
                );
                continue;
            }
            if !dry_run
                && let Err(e) = self
                    .gc_reconcile
                    .cascade_delete_dependents(
                        PodIdentity::new(ns, &res_name, &owner_uid),
                        self as &dyn GcPodDeleteSink,
                    )
                    .await
            {
                self.metrics.record_cascade_delete_failure();
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

fn initial_create_scheduling_state(pod: &Value) -> InitialPodSchedulingState {
    let explicit_node_name = pod
        .pointer("/spec/nodeName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());
    if let Some(explicit_node_name) = explicit_node_name {
        InitialPodSchedulingState {
            node_name: Some(explicit_node_name.to_string()),
            pending: false,
        }
    } else {
        InitialPodSchedulingState {
            node_name: None,
            pending: true,
        }
    }
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

fn upsert_pod_scheduled_true(pod: &mut Value, transition_time: &str) -> Result<(), AppError> {
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
        "lastTransitionTime": transition_time
    }));
    Ok(())
}

async fn apply_priority_class_to_pod(
    resources: &(impl AdmissionResourceStore + ?Sized),
    pod: &mut Value,
) -> Result<(), AppError> {
    let class_name = pod
        .pointer("/spec/priorityClassName")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(ToString::to_string);
    let (priority, policy) = resolve_priority_class(resources, class_name.as_deref()).await?;
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
    resources: &(impl AdmissionResourceStore + ?Sized),
    class_name: Option<&str>,
) -> Result<(Option<i64>, Option<String>), AppError> {
    match class_name {
        Some("system-node-critical") => Ok((Some(2_000_001_000_i64), None)),
        Some("system-cluster-critical") => Ok((Some(2_000_000_000_i64), None)),
        Some(class_name) => {
            let pc = resources
                .get_admission_resource("scheduling.k8s.io/v1", "PriorityClass", None, class_name)
                .await?;
            Ok(priority_class_value_and_policy(
                pc.as_ref().map(|pc| pc.data.as_ref()),
            ))
        }
        None => {
            let classes = resources
                .list_admission_resources("scheduling.k8s.io/v1", "PriorityClass", None)
                .await?;
            let default_class = classes.iter().find(|pc| {
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

fn patch_type_to_content_type(p: PodStatusPatchType) -> &'static str {
    match p {
        PodStatusPatchType::JsonPatch => "application/json-patch+json",
        PodStatusPatchType::MergePatch => "application/merge-patch+json",
        PodStatusPatchType::StrategicMerge => "application/strategic-merge-patch+json",
        PodStatusPatchType::ApplyPatch => "application/apply-patch+yaml",
    }
}

pub(crate) fn map_api_error_to_pod_repository(
    error: AppError,
    namespace: &str,
    name: &str,
) -> PodRepositoryError {
    match error {
        AppError::NotFound(_) => PodRepositoryError::not_found(namespace, name),
        AppError::BadRequest(message) => PodRepositoryError::invalid_request("pod", message),
        AppError::UnprocessableEntity(message) => PodRepositoryError::unprocessable(message),
        AppError::AlreadyExists(message) => PodRepositoryError::already_exists(message),
        AppError::Conflict(message) => PodRepositoryError::conflict(message),
        AppError::Forbidden(message) => PodRepositoryError::forbidden(message),
        AppError::ServiceUnavailable(message) => PodRepositoryError::unavailable(message),
        AppError::InternalError(message) | AppError::Internal(message) => {
            PodRepositoryError::internal(message)
        }
        AppError::Status {
            reason: "NotFound", ..
        } => PodRepositoryError::not_found(namespace, name),
        AppError::Status {
            reason: "AlreadyExists",
            message,
            ..
        } => PodRepositoryError::already_exists(message),
        AppError::Status {
            reason: "Conflict",
            message,
            ..
        } => PodRepositoryError::conflict(message),
        AppError::Status {
            reason: "Forbidden",
            message,
            ..
        } => PodRepositoryError::forbidden(message),
        AppError::Status { code, message, .. } if code == axum::http::StatusCode::BAD_REQUEST => {
            PodRepositoryError::invalid_request("pod", message)
        }
        AppError::Status { code, .. } if code == axum::http::StatusCode::NOT_FOUND => {
            PodRepositoryError::not_found(namespace, name)
        }
        AppError::Status { code, message, .. } if code == axum::http::StatusCode::FORBIDDEN => {
            PodRepositoryError::forbidden(message)
        }
        AppError::Status { code, message, .. } if code == axum::http::StatusCode::CONFLICT => {
            PodRepositoryError::conflict(message)
        }
        AppError::Status { code, message, .. }
            if code == axum::http::StatusCode::UNPROCESSABLE_ENTITY =>
        {
            PodRepositoryError::unprocessable(message)
        }
        AppError::Status { code, message, .. }
            if code == axum::http::StatusCode::INTERNAL_SERVER_ERROR =>
        {
            PodRepositoryError::internal(message)
        }
        AppError::Status { code, message, .. }
            if code == axum::http::StatusCode::SERVICE_UNAVAILABLE =>
        {
            PodRepositoryError::unavailable(message)
        }
        other => PodRepositoryError::unavailable(format!("{other:?}")),
    }
}

impl klights_pod_api::PodApiMutation for PodNativeOrchestration {
    fn create_pod(
        &self,
        request: klights_pod_api::PodApiCreateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiCreateResult> {
        Box::pin(async move {
            let namespace = request.namespace.clone();
            let result = self
                .api_create_pod(PodApiCreateRequest {
                    namespace: request.namespace,
                    name: String::new(),
                    body: request.body,
                    dry_run: request.dry_run,
                    run_admission: true,
                })
                .await
                .map_err(|error| map_api_error_to_pod_repository(error, &namespace, ""))?;
            Ok(klights_pod_api::PodApiCreateResult {
                resource: result.resource,
                body: result.body,
            })
        })
    }

    fn update_pod(
        &self,
        request: klights_pod_api::PodApiUpdateRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        Box::pin(async move {
            match self
                .api_update_pod(
                    &request.namespace,
                    &request.name,
                    request.body,
                    request.current,
                    request.dry_run,
                )
                .await
                .map_err(|error| {
                    map_api_error_to_pod_repository(error, &request.namespace, &request.name)
                })? {
                PodApiUpdateOutcome::Persisted(resource) => {
                    Ok(klights_pod_api::PodApiWriteOutcome::Persisted(resource))
                }
                PodApiUpdateOutcome::DryRun(value) => {
                    Ok(klights_pod_api::PodApiWriteOutcome::DryRun(value))
                }
            }
        })
    }

    fn patch_pod(
        &self,
        request: klights_pod_api::PodApiPatchRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiWriteOutcome> {
        Box::pin(async move {
            let patch_type = match request.patch_kind {
                klights_pod_api::PodStatusPatchKind::JsonPatch => PodStatusPatchType::JsonPatch,
                klights_pod_api::PodStatusPatchKind::MergePatch => PodStatusPatchType::MergePatch,
                klights_pod_api::PodStatusPatchKind::StrategicMerge => {
                    PodStatusPatchType::StrategicMerge
                }
                klights_pod_api::PodStatusPatchKind::ApplyPatch => PodStatusPatchType::ApplyPatch,
            };
            match self
                .api_patch_pod(
                    &request.namespace,
                    &request.name,
                    request.patch,
                    patch_type,
                    request.dry_run,
                )
                .await
                .map_err(|error| {
                    map_api_error_to_pod_repository(error, &request.namespace, &request.name)
                })? {
                PodApiUpdateOutcome::Persisted(resource) => {
                    Ok(klights_pod_api::PodApiWriteOutcome::Persisted(resource))
                }
                PodApiUpdateOutcome::DryRun(value) => {
                    Ok(klights_pod_api::PodApiWriteOutcome::DryRun(value))
                }
            }
        })
    }

    fn delete_pod(
        &self,
        request: klights_pod_api::PodApiDeleteRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, klights_pod_api::PodApiDeleteOutcome> {
        Box::pin(async move {
            match self
                .repository_delete_pod(
                    &request.namespace,
                    &request.name,
                    request.options,
                    request.dry_run,
                )
                .await?
            {
                PodApiDeleteOutcome::GracefulSet(resource) => {
                    Ok(klights_pod_api::PodApiDeleteOutcome::GracefulSet(resource))
                }
                PodApiDeleteOutcome::DryRun(value) => {
                    Ok(klights_pod_api::PodApiDeleteOutcome::DryRun(value))
                }
            }
        })
    }

    fn delete_collection_pods(
        &self,
        request: klights_pod_api::PodApiDeleteCollectionRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            self.api_delete_collection_pods(
                &request.namespace,
                request.label_selector.as_deref(),
                request.field_selector.as_deref(),
                request.dry_run,
            )
            .await
            .map_err(|error| map_api_error_to_pod_repository(error, &request.namespace, ""))
        })
    }

    fn bind_pod(
        &self,
        request: klights_pod_api::PodBindingRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, ()> {
        Box::pin(async move {
            self.bind_pod_from_api(
                &request.namespace,
                &request.name,
                request.binding,
                request.dry_run,
            )
            .await
            .map_err(|error| {
                map_api_error_to_pod_repository(error, &request.namespace, &request.name)
            })
        })
    }
}

impl klights_kubelet::pod_repository::PodTerminationPort for PodNativeOrchestration {
    fn mark_terminating(
        &self,
        target: klights_pod_api::PodMutationTarget,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        Box::pin(async move { self.mark_pod_terminating_for_repository(&target).await })
    }
}

impl klights_pod_api::PodMarkTerminating for PodNativeOrchestration {
    fn mark_pod_terminating(
        &self,
        request: klights_pod_api::PodMarkTerminatingRequest,
    ) -> klights_pod_api::PodRepositoryFuture<'_, Resource> {
        klights_kubelet::pod_repository::PodRepositoryService::mark_pod_terminating_from(
            self, request,
        )
    }
}

impl GcPodDeleteSink for PodNativeOrchestration {
    fn request_gc_pod_delete(&self, request: GcPodDeleteRequest) -> GcPodDeleteFuture<'_> {
        Box::pin(async move {
            let identity = request.into_identity();
            let options = crate::api::DeleteOptions::with_uid_precondition(&identity.uid);
            match self
                .api_delete_pod_for_gc(&identity.namespace, &identity.name, options, false)
                .await
            {
                Ok(_outcome) => Ok(()),
                Err(error) => {
                    Err(
                        classify_gc_pod_delete_error(self.pod_query.as_ref(), &identity, error)
                            .await,
                    )
                }
            }
        })
    }
}

pub(crate) async fn classify_gc_pod_delete_error(
    query: &dyn PodQuery,
    identity: &PodIdentity,
    error: AppError,
) -> GcPodDeleteError {
    match error {
        AppError::NotFound(message) => GcPodDeleteError::not_found(message),
        AppError::Status {
            reason: "NotFound",
            message,
            ..
        } => GcPodDeleteError::not_found(message),
        AppError::Conflict(message)
        | AppError::Status {
            reason: "Conflict",
            message,
            ..
        } => match query
            .get_pod(
                PodGetRequest::try_by_name(identity.namespace.clone(), identity.name.clone())
                    .expect("Pod identity has a validated namespace and name"),
            )
            .await
        {
            Ok(None) => GcPodDeleteError::not_found(format!(
                "Pod {}/{} disappeared after delete conflict: {message}",
                identity.namespace, identity.name
            )),
            Ok(Some(current)) if current.uid.is_empty() || identity.uid.is_empty() => {
                GcPodDeleteError::unavailable(format!(
                    "could not establish Pod identity after delete conflict: {message}"
                ))
            }
            Ok(Some(current)) if current.uid != identity.uid => {
                GcPodDeleteError::identity_changed(format!(
                    "Pod {}/{} identity changed from {} to {}: {message}",
                    identity.namespace, identity.name, identity.uid, current.uid
                ))
            }
            Ok(Some(_)) => GcPodDeleteError::unavailable(format!(
                "Pod delete conflicted while the requested UID is still current: {message}"
            )),
            Err(read_error) => GcPodDeleteError::unavailable(format!(
                "Pod delete conflicted and identity re-read failed ({read_error}): {message}"
            )),
        },
        other => GcPodDeleteError::unavailable(format!("{other:?}")),
    }
}
