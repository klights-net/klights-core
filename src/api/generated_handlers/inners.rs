//! Inner CRUD handler functions (list/get/create/update/delete/patch/delete_collection).
//! Extracted from generated_handlers.rs (refactor).

use crate::api::*;
use crate::datastore::{DatastoreBackend, Resource, ResourcePreconditions};
use std::sync::Arc;

use super::helpers::*;
use crate::api::mutation::DryRunMode;
use crate::api::mutation::write::{
    CreateStrategy, PatchStrategy, UpdateStrategy, WriteResult, create_with_strategy,
    patch_with_strategy, update_with_strategy,
};
use crate::kubelet::pod_repository::PodApiWriter;
use klights_reconcile_api::MutationOperation;

pub use crate::api::finalizer_delete::DeleteCompletion;

pub struct GeneratedListInnerRequest {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub list_kind: &'static str,
    pub namespace: Option<String>,
    pub query: ListQuery,
    pub headers: HeaderMap,
}

pub struct GeneratedNamedResource<'a> {
    pub api_version: &'static str,
    pub kind: &'static str,
    pub namespace: Option<&'a str>,
    pub name: &'a str,
}

impl<'a> GeneratedNamedResource<'a> {
    pub fn new(
        api_version: &'static str,
        kind: &'static str,
        namespace: Option<&'a str>,
        name: &'a str,
    ) -> Self {
        Self {
            api_version,
            kind,
            namespace,
            name,
        }
    }
}

pub struct GeneratedUpdateInnerRequest<'a> {
    pub target: GeneratedNamedResource<'a>,
    pub query: CreateUpdateQuery,
    pub body: Value,
}

pub struct GeneratedDeleteInnerRequest<'a> {
    pub target: GeneratedNamedResource<'a>,
    pub query: CreateUpdateQuery,
    pub body: Bytes,
}

pub struct GeneratedPatchInnerRequest<'a> {
    pub target: GeneratedNamedResource<'a>,
    pub query: CreateUpdateQuery,
    pub headers: HeaderMap,
    pub body: Bytes,
}

pub struct GeneratedDeleteCompletionRequest<'a> {
    pub target: crate::api::finalizer_delete::ResourceDeleteTarget<'a>,
    pub initial_resource: Resource,
    pub delete_preconditions: ResourcePreconditions,
    pub orphan_children_before_completion: bool,
    pub uid_mismatch_is_conflict: bool,
}

async fn enqueue_generated_controller_after_mutation(
    state: &AppState,
    api_version: &'static str,
    kind: &'static str,
    resource: &Value,
) {
    if crate::controllers::workqueue::controller_kind_static(api_version, kind).is_some() {
        state.controller_dispatcher.enqueue(resource).await;
    }
}

async fn maybe_reconcile_cluster_role_aggregation(
    state: &Arc<AppState>,
    api_version: &'static str,
    kind: &'static str,
) {
    if (api_version, kind) != ("rbac.authorization.k8s.io/v1", "ClusterRole") {
        return;
    }

    if let Err(err) =
        crate::controllers::rbac_reconcile::reconcile_cluster_role_aggregation(state.db.as_ref())
            .await
    {
        tracing::warn!(
            error = %err,
            "failed to reconcile ClusterRole aggregation after mutation"
        );
    }
}

async fn schedule_foreground_owner_finalization(
    state: &Arc<AppState>,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    name: &str,
    owner: Resource,
) {
    let dispatch_state = state.clone();
    let namespace = namespace.map(str::to_string);
    let name = name.to_string();
    let dispatch_namespace = namespace.clone();
    let dispatch_name = name.clone();

    if let Err(err) = state
        .task_supervisor
        .spawn_async(
            klights_supervisor::TaskCategory::Background,
            "foreground_owner_finalization_dispatch",
            async move {
                let worker_state = dispatch_state.clone();
                let worker_namespace = dispatch_namespace.clone();
                let worker_name = dispatch_name.clone();
                let worker_owner = owner;
                let worker_supervisor = dispatch_state.task_supervisor.clone();
                let worker_metrics = dispatch_state.metrics.clone();
                if let Err(err) = worker_supervisor
                    .spawn_async(
                        klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                        "foreground_owner_finalization",
                        async move {
                            if let Err(err) = controllers::gc::finalize_foreground_owner_if_ready(
                                worker_state.db.as_ref(),
                                &worker_owner,
                                worker_state.pod_repository.as_ref()
                                    as &dyn klights_reconcile_api::GcPodDeleteSink,
                            )
                            .await
                            {
                                worker_metrics
                                    .cascade_delete_failures_total
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                tracing::error!(
                                    namespace = ?worker_namespace,
                                    name = %worker_name,
                                    api_version = %api_version,
                                    kind = %kind,
                                    error = %err,
                                    "foreground owner finalization failed"
                                );
                            }
                        },
                    )
                    .await
                {
                    dispatch_state
                        .metrics
                        .cascade_delete_failures_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::warn!(
                        namespace = ?dispatch_namespace,
                        name = %dispatch_name,
                        api_version = %api_version,
                        kind = %kind,
                        error = %err,
                        "failed to schedule foreground owner finalization work task"
                    );
                }
            },
        )
        .await
    {
        state
            .metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::warn!(
            namespace = ?namespace,
            name = %name,
            api_version = %api_version,
            kind = %kind,
            error = %err,
            "failed to schedule foreground owner finalization dispatch task"
        );
    }
}

async fn dispatch_generated_mutation_event(
    state: &Arc<AppState>,
    operation: klights_reconcile_api::MutationOperation,
    resource: &Value,
    context: &'static str,
) {
    crate::api::mutation::dispatch_mutation_event(
        &state.side_effects,
        state.db.as_ref(),
        &state.metrics,
        crate::api::mutation::MutationEvent {
            operation,
            resource,
            old_resource: None,
            persisted: true,
            dry_run: crate::api::mutation::DryRunMode::Live,
            context,
        },
    )
    .await;
}

pub async fn mark_foreground_deletion_with_retry(
    db: &dyn DatastoreBackend,
    api_version: &str,
    kind: &str,
    ns: Option<&str>,
    name: &str,
    initial_resource: Resource,
    delete_preconditions: ResourcePreconditions,
) -> Result<Resource, AppError> {
    crate::api::finalizer_delete::mark_foreground_deletion_with_retry(
        db,
        api_version,
        kind,
        ns,
        name,
        initial_resource,
        delete_preconditions,
    )
    .await
}

pub async fn complete_non_foreground_delete_with_live_recheck(
    db: &dyn DatastoreBackend,
    request: GeneratedDeleteCompletionRequest<'_>,
) -> Result<DeleteCompletion, AppError> {
    crate::api::finalizer_delete::complete_non_foreground_delete_with_live_recheck(
        db,
        crate::api::finalizer_delete::NonForegroundDeleteRequest {
            target: request.target,
            initial_resource: request.initial_resource,
            delete_preconditions: request.delete_preconditions,
            orphan_children_before_completion: request.orphan_children_before_completion,
            uid_mismatch_is_conflict: request.uid_mismatch_is_conflict,
            grace_seconds: 0,
        },
    )
    .await
}

pub async fn delete_collection_listed_resource_inner(
    state: Arc<AppState>,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    resource: Resource,
) -> Result<bool, AppError> {
    let resource_name = resource.name.clone();
    let resource_uid = resource.uid.clone();
    let delete_strategy = crate::api::mutation::delete::FinalizerAwareDeleteStrategy {
        db: state.db.as_ref(),
    };
    let target_identity = klights_types::ResourceKey::new(
        api_version,
        kind,
        namespace.map(str::to_string),
        resource_name.clone(),
    );
    let item_intent = crate::api::mutation::DeleteIntent::collection_item(
        crate::api::mutation::DryRunMode::Live,
        ResourcePreconditions::uid(resource_uid),
    );
    match crate::api::mutation::delete::delete_loaded_with_strategy(
        &delete_strategy,
        target_identity,
        resource,
        &item_intent,
    )
    .await?
    {
        crate::api::mutation::delete::DeleteResult::HardDeleted(resource) => {
            if api_version == "v1"
                && kind == "Node"
                && let Err(err) = state
                    .db
                    .delete_pod_cleanup_intents_for_node(&resource.name)
                    .await
            {
                tracing::warn!(
                    node = %resource.name,
                    error = %err,
                    "failed to delete pod cleanup intents for deleted node"
                );
            }
            Ok(true)
        }
        crate::api::mutation::delete::DeleteResult::MarkedTerminating(_)
        | crate::api::mutation::delete::DeleteResult::GoneOrUidChanged => Ok(false),
    }
}

async fn run_post_hard_delete_effects(
    state: &Arc<AppState>,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    resource: &Resource,
    cascade: bool,
) {
    crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
        state,
        api_version,
        kind,
    )
    .await;

    if api_version == "v1" && kind == "Service" {
        crate::controllers::service::release_service_allocations_from_resource(
            state.service_ipam.as_ref(),
            state.nodeport_alloc.as_ref(),
            &resource.data,
        );
    }

    dispatch_generated_mutation_event(
        state,
        klights_reconcile_api::MutationOperation::HardDelete,
        &resource.data,
        "generated_hard_delete",
    )
    .await;

    if !cascade {
        return;
    }

    if let Err(e) = controllers::gc::cascade_delete_with_uid(
        state.db.as_ref(),
        &resource.uid,
        api_version,
        &resource.name,
        kind,
        namespace.map(str::to_string),
        state.pod_repository.as_ref() as &dyn klights_reconcile_api::GcPodDeleteSink,
    )
    .await
    {
        state
            .metrics
            .cascade_delete_failures_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::error!(namespace = ?namespace, name = %resource.name, error = %e, "cascade delete failed");
    }
}

pub async fn list_inner(
    state: Arc<AppState>,
    _identity: &crate::auth::AuthenticatedIdentity,
    request: GeneratedListInnerRequest,
) -> Result<Response, AppError> {
    let GeneratedListInnerRequest {
        api_version,
        kind,
        list_kind,
        namespace,
        query,
        headers,
    } = request;
    let ns = namespace.as_deref();
    validate_builtin_field_selector(
        api_version,
        kind,
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        ns.is_some(),
    )?;
    if query.watch == Some("true".to_string()) {
        query.validate_send_initial_events_watch()?;
        let send_initial_events = query.send_initial_events.as_deref() == Some("true");
        let explicit_resource_version_zero = query
            .resource_version
            .as_deref()
            .is_some_and(|rv| rv.trim() == "0");
        let requested_rv: i64 = query
            .resource_version
            .as_ref()
            .and_then(|rv| rv.parse::<i64>().ok())
            .unwrap_or(0);

        let kind_owned = kind.to_string();
        let ns_owned = ns.map(str::to_string);
        let db = state.db.clone();
        let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
        let table_format = wants_table_format(&headers)?;
        let protobuf_supported = protobuf_watch_supported_for_request(
            api_version,
            kind,
            table_format,
            query.label_selector.as_deref(),
            query.field_selector.as_deref(),
        );
        let stream_format = negotiate_watch_stream_format(&headers, protobuf_supported)?;
        let label_selector = query.label_selector.clone();
        let field_selector = query.field_selector.clone();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            db,
            task_supervisor: state.task_supervisor.clone(),
            api_version,
            kind: kind_owned,
            watch_namespace: ns_owned,
            requested_rv,
            send_initial_events,
            send_bookmarks,
            label_selector,
            field_selector,
            table_format,
            stream_format,
            timeout_seconds: query.timeout_seconds,
            emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
        })
        .await;
        return Ok(Response::builder()
            .header("Content-Type", stream_format.content_type())
            .header("Transfer-Encoding", "chunked")
            .body(body)
            .unwrap());
    }

    let normalized_limit = query.normalized_limit()?;

    // Validate and resolve resourceVersion / resourceVersionMatch for the plain
    // (non-watch) LIST. Honors rv=0 cache reads, NotOlderThan, and Exact; 400s
    // on unsupported match values / illegal combinations.
    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|t| !t.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;

    let (db_continue_name, continue_resource_version) =
        process_continue_token(query.continue_token)?;

    let list_query = crate::datastore::ResourceListQuery::new(
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        normalized_limit,
        db_continue_name.as_deref(),
    );

    // Consistent-snapshot selection (pin Exact / session continuations, downgrade
    // a continuation that outran the retained window, honor resourceVersionMatch)
    // is shared across every list handler — see `query::resolve_list_page`.
    let db_for_snapshot = state.db.clone();
    let db_for_live = state.db.clone();
    let crate::api::query::ResolvedListPage {
        list,
        response_rv,
        continue_resource_version,
    } = crate::api::query::resolve_list_page(
        state.db.as_ref(),
        rv_match,
        continue_resource_version,
        |srv| async move {
            db_for_snapshot
                .snapshot_resources_at_rv(api_version, kind, ns, list_query, srv)
                .await
                .map_err(AppError::from)
        },
        || async move {
            db_for_live
                .list_resources(api_version, kind, ns, list_query)
                .await
                .map_err(AppError::from)
        },
    )
    .await?;

    let mut items: Vec<Value> = Vec::with_capacity(list.items.len());
    for r in list.items {
        let mut data = inject_resource_version(r.data, r.resource_version);
        normalize_resource_for_read(api_version, kind, &mut data);
        inject_node_last_heartbeat_on_leader(&state, api_version, kind, &mut data).await;
        items.push(data);
    }
    let resource_version = response_rv.to_string();

    if wants_table_format(&headers)? {
        let table = match kind {
            "Pod" => pod_list_to_table(items, resource_version),
            "Node" => node_list_to_table(items, resource_version),
            "ReplicaSet" => replicaset_list_to_table(items, resource_version),
            "Deployment" => deployment_list_to_table(items, resource_version),
            "StatefulSet" => statefulset_list_to_table(items, resource_version),
            // Resources without a dedicated converter use kubectl's per-kind
            // columns, falling back to the upstream default (NAME + CREATED AT).
            _ => crate::api::response::generic_list_to_table(kind, items, resource_version),
        };
        return Ok(Json(table).into_response());
    }

    let mut metadata = serde_json::json!({
        "resourceVersion": resource_version,
    });
    if let Some(ref name) = list.continue_token {
        let token = crate::api::query::encode_response_continue_token(
            name,
            response_rv,
            continue_resource_version,
        );
        metadata["continue"] = serde_json::json!(token);
    }
    if let Some(remaining) = list.remaining_item_count {
        metadata["remainingItemCount"] = serde_json::json!(remaining);
    }
    let response = serde_json::json!({
        "apiVersion": api_version,
        "kind": list_kind,
        "metadata": metadata,
        "items": items,
    });

    Ok(K8sResponse::new(response, &headers).into_response())
}

pub async fn get_inner(
    state: Arc<AppState>,
    _identity: &crate::auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    ns: Option<&str>,
    name: &str,
    headers: HeaderMap,
) -> Result<K8sResponse, AppError> {
    match state.db.get_resource(api_version, kind, ns, name).await? {
        Some(resource) => {
            let resource = if api_version == "v1" && kind == "Secret" {
                crate::bootstrap::bootstrap_token::rotate_bootstrap_token_secret_for_get(
                    state.db.as_ref(),
                    &resource,
                )
                .await?
            } else {
                resource
            };
            let mut data = inject_resource_version(resource.data, resource.resource_version);
            normalize_resource_for_read(api_version, kind, &mut data);
            inject_node_last_heartbeat_on_leader(&state, api_version, kind, &mut data).await;
            Ok(K8sResponse::new(data, &headers))
        }
        None => Err(AppError::not_found(api_version, kind, name)),
    }
}

async fn inject_node_last_heartbeat_on_leader(
    state: &AppState,
    api_version: &str,
    kind: &str,
    node: &mut Value,
) {
    if api_version != "v1" || kind != "Node" {
        return;
    }

    let node_name = node
        .pointer("/metadata/name")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    let Some(conditions) = node
        .pointer_mut("/status/conditions")
        .and_then(|value| value.as_array_mut())
    else {
        return;
    };
    let Some(ready) = conditions
        .iter_mut()
        .find(|condition| condition.get("type").and_then(|value| value.as_str()) == Some("Ready"))
    else {
        return;
    };

    if let Some(obj) = ready.as_object_mut() {
        obj.remove("lastHeartbeatTime");
    }

    let is_raft_leader = state
        .is_raft_leader_rx
        .as_ref()
        .is_some_and(|proxy| proxy.is_leader());
    if !is_raft_leader {
        return;
    }

    let Some(node_name) = node_name.as_deref() else {
        return;
    };

    if let Some(observation) = state.node_lease_tracker.observed(node_name).await {
        ready["lastHeartbeatTime"] = serde_json::json!(observation.renew_time_string());
    }
}

struct BuiltinCreateStrategy<'a> {
    state: &'a Arc<AppState>,
    identity: &'a crate::auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&'a str>,
    query: &'a CreateUpdateQuery,
}

impl<'a> BuiltinCreateStrategy<'a> {
    fn is_generated_pod_create(&self) -> bool {
        self.api_version == "v1" && self.kind == "Pod" && self.namespace.is_some()
    }
}

#[async_trait::async_trait]
impl<'a> CreateStrategy for BuiltinCreateStrategy<'a> {
    async fn before_admission(&self, mut body: Value) -> Result<Value, AppError> {
        if let Some(namespace) = self.namespace {
            reject_if_namespace_missing_or_terminating(self.state.db.as_ref(), namespace).await?;
        }

        check_field_validation_strict_typed(self.api_version, self.kind, self.query, &body)?;

        // Validate explicit metadata.name before admission. Empty names are
        // resolved from generateName later.
        if let Some(name) = body
            .get("metadata")
            .and_then(|m| m.get("name"))
            .and_then(|n| n.as_str())
            .filter(|s| !s.is_empty())
        {
            crate::api::validation::validate_metadata_name_for_kind(
                self.api_version,
                self.kind,
                name,
                &format!("metadata.name for {}", self.kind),
            )?;
        }

        if self.is_generated_pod_create() {
            return Ok(body);
        }

        if self.kind == "Pod" {
            if let Err(msg) = crate::kubelet::volumes::validate_volume_subpaths(&body) {
                return Err(AppError::UnprocessableEntity(msg));
            }
            if let Err(msg) = crate::kubelet::volumes::validate_volume_projection_paths(&body) {
                return Err(AppError::UnprocessableEntity(msg));
            }
            validate_pod_sysctls(&body)?;
        }

        // CSR create: server-fill spec identity fields from authenticated identity.
        // Clients must not be able to forge these per Kubernetes semantics.
        if self.kind == "CertificateSigningRequest" {
            stamp_csr_identity(&mut body, self.identity);
        }

        prepare_admissionregistration_resource(self.kind, &mut body)?;

        // RBAC privilege-escalation / bind enforcement (k8s parity): a user may not
        // create a Role/ClusterRole or (Cluster)RoleBinding granting more than they
        // hold, absent the escalate/bind verb.
        crate::api::rbac_admission::enforce_rbac_write_authorization(
            self.state,
            self.identity,
            self.api_version,
            self.kind,
            self.namespace,
            &body,
        )
        .await?;

        Ok(body)
    }

    async fn admit(&self, body: Value, dry_run: DryRunMode) -> Result<Value, AppError> {
        if self.is_generated_pod_create() {
            return Ok(body);
        }
        let is_dry_run = dry_run.is_all();
        crate::api::mutation::write::run_admission(
            self.state.db.as_ref(),
            AdmissionContextRequest {
                api_version: self.api_version,
                kind: self.kind,
                operation: "CREATE",
                namespace: self.namespace.map(str::to_string),
                name: body
                    .get("metadata")
                    .and_then(|m| m.get("name"))
                    .and_then(|n| n.as_str())
                    .map(ToString::to_string),
                object: body,
                old_object: None,
                dry_run: is_dry_run,
                subresource: None,
                options: None,
            },
        )
        .await
    }

    async fn persist_create(
        &self,
        mut body: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let is_dry_run = dry_run.is_all();
        let ns = self.namespace;
        let api_version = self.api_version;
        let kind = self.kind;

        if self.is_generated_pod_create() {
            // Generated Pod create delegates to PodApiWriter which owns Pod
            // admission/defaulting/persistence.
            let namespace = ns.unwrap();
            let resource_name = body
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let result = self
                .state
                .pod_repository
                .api_create_pod(crate::kubelet::pod_repository::PodApiCreateRequest {
                    namespace: namespace.to_string(),
                    name: String::new(),
                    body,
                    dry_run: is_dry_run,
                    run_admission: true,
                })
                .await
                .map_err(|error| {
                    AppError::from(error).with_resource_context("v1", "Pod", &resource_name)
                })?;
            return match result.resource {
                Some(resource) => Ok(WriteResult::Persisted(resource)),
                None => Ok(WriteResult::DryRun(result.body)),
            };
        }

        if kind == "Pod"
            && let Some(namespace) = ns
        {
            apply_pod_runtimeclass_admission(self.state.db.as_ref(), &mut body).await?;
            apply_limitrange_defaults_to_pod(self.state.db.as_ref(), namespace, &mut body).await?;
            enforce_limitrange_constraints_for_pod(self.state.db.as_ref(), namespace, &body)
                .await?;
        }
        if kind == "PersistentVolumeClaim"
            && let Some(namespace) = ns
        {
            apply_default_storage_class_admission(self.state.db.as_ref(), &mut body).await?;
            enforce_limitrange_constraints_for_pvc(self.state.db.as_ref(), namespace, &body)
                .await?;
        }

        validate_builtin_resource_spec(kind, &body)?;

        if is_dry_run {
            return Ok(WriteResult::DryRun(body));
        }

        if kind != "ResourceQuota"
            && let Some(namespace) = ns
        {
            check_resource_quota_for_creation(self.state.db.as_ref(), namespace, kind, &body)
                .await?;
        }

        let resource_name = resolve_resource_name(&mut body)?;

        if !validate_metadata_name_for_kind(api_version, kind, &resource_name) {
            let detail = if metadata_name_uses_path_segment_validation(api_version, kind)
                || kind == "IPAddress"
            {
                "must be a valid path segment (not '.', '..', and no '/' or '%')"
            } else {
                "must be a valid DNS subdomain (lowercase alphanumeric, hyphens, dots; max 253 chars; cannot start/end with hyphen or dot)"
            };
            return Err(AppError::UnprocessableEntity(format!(
                "Invalid metadata.name '{}': {}",
                resource_name, detail
            )));
        }

        crate::api::mutation::write::prepare_create_metadata(ns, &mut body, &resource_name);

        match kind {
            "Pod" => apply_pod_create_defaults(&mut body),
            "PersistentVolumeClaim" => apply_pvc_create_defaults(&mut body),
            "PersistentVolume" => apply_pv_create_defaults(&mut body),
            "Namespace" => ensure_namespace_status_phase_active(&mut body),
            "ResourceQuota" => apply_resourcequota_create_status(&mut body),
            _ => {}
        }

        apply_workload_replicas_default(kind, &mut body);
        if kind == "ReplicationController" {
            apply_replicationcontroller_selector_default(&mut body);
        }
        if kind == "StatefulSet" {
            initialize_statefulset_revision_status_on_create(&resource_name, &mut body);
        }

        if kind == "Secret" {
            if let Err(err_msg) = validate_secret_data(&body) {
                return Err(AppError::UnprocessableEntity(err_msg));
            }
            process_secret_stringdata(&mut body);
        }

        normalize_resource_for_storage(api_version, kind, &mut body);

        let pending_service_allocations = if api_version == "v1" && kind == "Service" {
            Some(
                crate::controllers::service::prepare_service_for_create(
                    self.state.db.as_ref(),
                    &mut body,
                    self.state.service_ipam.as_ref(),
                    self.state.nodeport_alloc.as_ref(),
                )
                .await
                .map_err(|e| {
                    AppError::Internal(format!("Failed to allocate service fields: {e}"))
                })?,
            )
        } else {
            None
        };

        let resource = match self
            .state
            .db
            .create_resource(api_version, kind, ns, &resource_name, body)
            .await
        {
            Ok(resource) => resource,
            Err(e) => {
                if let Some(pending) = pending_service_allocations {
                    pending.release(
                        self.state.service_ipam.as_ref(),
                        self.state.nodeport_alloc.as_ref(),
                    );
                }
                // Attach details.{group,kind,name} to AlreadyExists/Conflict.
                return Err(AppError::from(e).with_resource_context(
                    api_version,
                    kind,
                    &resource_name,
                ));
            }
        };

        Ok(WriteResult::Persisted(resource))
    }
}

struct BuiltinUpdateStrategy<'a> {
    state: &'a Arc<AppState>,
    identity: &'a crate::auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&'a str>,
    name: &'a str,
    query: &'a CreateUpdateQuery,
}

#[async_trait::async_trait]
impl<'a> UpdateStrategy for BuiltinUpdateStrategy<'a> {
    async fn load_current(&self) -> Result<Resource, AppError> {
        self.state
            .db
            .get_resource(self.api_version, self.kind, self.namespace, self.name)
            .await?
            .ok_or_else(|| AppError::NotFound(format!("{} not found", self.kind)))
    }

    async fn prepare_update(
        &self,
        current: &Resource,
        mut body: Value,
        dry_run: DryRunMode,
    ) -> Result<Value, AppError> {
        let kind = self.kind;
        let ns = self.namespace;

        if (kind == "ConfigMap" || kind == "Secret")
            && current
                .data
                .get("immutable")
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        {
            let ns_str = ns.unwrap_or("");
            check_immutable_fields(&current.data, &body, kind, ns_str, self.name)?;
        }

        let _ = dry_run;

        check_field_validation_strict_typed(self.api_version, kind, self.query, &body)?;

        if kind == "Pod" {
            if let Err(msg) = crate::kubelet::volumes::validate_volume_subpaths(&body) {
                return Err(AppError::UnprocessableEntity(msg));
            }
            if let Err(msg) = crate::kubelet::volumes::validate_volume_projection_paths(&body) {
                return Err(AppError::UnprocessableEntity(msg));
            }
            validate_pod_sysctls(&body)?;
        }

        prepare_admissionregistration_resource(kind, &mut body)?;

        // RBAC privilege-escalation / bind enforcement (k8s parity) on update.
        crate::api::rbac_admission::enforce_rbac_write_authorization(
            self.state,
            self.identity,
            self.api_version,
            kind,
            ns,
            &body,
        )
        .await?;

        body = crate::api::mutation::write::run_admission(
            self.state.db.as_ref(),
            AdmissionContextRequest {
                api_version: self.api_version,
                kind,
                operation: "UPDATE",
                namespace: ns.map(str::to_string),
                name: Some(self.name.to_string()),
                object: body,
                old_object: Some((*current.data).clone()),
                dry_run: dry_run.is_all(),
                subresource: None,
                options: None,
            },
        )
        .await?;

        if kind == "Pod"
            && let Some(namespace) = ns
        {
            validate_pod_resource_requirements_immutable(&current.data, &body)?;
            check_resource_quota_for_pod_update(
                self.state.db.as_ref(),
                namespace,
                &current.data,
                &body,
            )
            .await?;
        }
        if kind == "PersistentVolumeClaim"
            && let Some(namespace) = ns
        {
            check_resource_quota_for_pvc_update(
                self.state.db.as_ref(),
                namespace,
                &current.data,
                &body,
            )
            .await?;
        }

        if kind == "PriorityClass" {
            validate_priorityclass_update_immutable(&current.data, &body)?;
        }

        validate_builtin_resource_spec(kind, &body)?;
        normalize_resource_for_storage(self.api_version, kind, &mut body);

        Ok(body)
    }

    async fn persist_update(
        &self,
        current: Resource,
        mut body: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let kind = self.kind;

        if kind == "Secret" {
            if let Err(err_msg) = validate_secret_data(&body) {
                return Err(AppError::UnprocessableEntity(err_msg));
            }
            process_secret_stringdata(&mut body);
        }

        let requested_rv = metadata_resource_version(&body);
        crate::api::mutation::write::prepare_builtin_generation_for_update(
            kind,
            &current.data,
            &mut body,
        );

        preserve_status_subresource_on_main_update(
            self.api_version,
            kind,
            &current.data,
            &mut body,
        );
        crate::api::finalizer_delete::preserve_deletion_timestamp_on_update(
            &current.data,
            &mut body,
        );

        if dry_run.is_all() {
            return Ok(WriteResult::DryRun(body));
        }

        let resource = self
            .state
            .db
            .update_main_resource_with_preconditions(
                self.api_version,
                kind,
                self.namespace,
                self.name,
                body.clone(),
                ResourcePreconditions {
                    uid: Some(current.uid.clone()),
                    resource_version: requested_rv,
                },
            )
            .await?;

        Ok(WriteResult::Persisted(resource))
    }
}

struct BuiltinPatchStrategy<'a> {
    state: &'a Arc<AppState>,
    identity: &'a crate::auth::AuthenticatedIdentity,
    target: GeneratedNamedResource<'a>,
    query: &'a CreateUpdateQuery,
    headers: &'a HeaderMap,
}

#[async_trait::async_trait]
impl<'a> PatchStrategy for BuiltinPatchStrategy<'a> {
    async fn apply_patch(
        &self,
        mut patch: Value,
        dry_run: DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let content_type = self
            .headers
            .get("content-type")
            .and_then(|h| h.to_str().ok());
        let is_apply = matches!(
            content_type,
            Some("application/apply-patch+yaml") | Some("application/apply-patch+json")
        );
        let supports_apply_create = content_type == Some("application/apply-patch+yaml");
        let apply_manager = crate::api::server_side_apply::resolve_field_manager(
            self.query.field_manager.as_deref(),
        );
        let apply_force = self.query.force.unwrap_or(false);
        let is_dry_run = dry_run.is_all();
        let api_version = self.target.api_version;
        let kind = self.target.kind;
        let ns = self.target.namespace;
        let name = self.target.name;

        if is_apply {
            check_field_validation_strict_typed(api_version, kind, self.query, &patch)?;
        }

        if supports_apply_create {
            let exists = self
                .state
                .db
                .get_resource(api_version, kind, ns, name)
                .await?
                .is_some();
            if !exists {
                // CSR apply-create: server-fill spec identity fields from the
                // authenticated identity. Clients must not be able to forge
                // spec.username/groups/uid/extra via server-side-apply (mirrors the
                // POST create path), or the auto-signer would mint certs for a
                // forged identity (e.g. system:node:<other>).
                if kind == "CertificateSigningRequest" {
                    stamp_csr_identity(&mut patch, self.identity);
                }

                // RBAC privilege-escalation / bind enforcement (k8s parity) on the
                // server-side-apply create path.
                crate::api::rbac_admission::enforce_rbac_write_authorization(
                    self.state,
                    self.identity,
                    api_version,
                    kind,
                    ns,
                    &patch,
                )
                .await?;
                // Server-Side Apply create: build the object (with managedFields)
                // from the apply config.
                let applied_object = crate::api::server_side_apply::server_side_apply(
                    None,
                    &patch,
                    &apply_manager,
                    api_version,
                    &crate::utils::k8s_time_now(),
                    apply_force,
                )
                .map_err(|conflicts| AppError::Conflict(conflicts.message()))?;
                let admitted = crate::api::mutation::write::run_admission(
                    self.state.db.as_ref(),
                    AdmissionContextRequest {
                        api_version,
                        kind,
                        operation: "CREATE",
                        namespace: ns.map(str::to_string),
                        name: Some(name.to_string()),
                        object: applied_object,
                        old_object: None,
                        dry_run: is_dry_run,
                        subresource: None,
                        options: None,
                    },
                )
                .await?;
                if let Some(namespace) = ns {
                    check_resource_quota_for_creation(
                        self.state.db.as_ref(),
                        namespace,
                        kind,
                        &admitted,
                    )
                    .await?;
                }
                let mut admitted_with_annot = admitted;
                normalize_resource_for_storage(api_version, kind, &mut admitted_with_annot);
                if is_dry_run {
                    return Ok(WriteResult::Response {
                        status: StatusCode::CREATED,
                        body: admitted_with_annot,
                    });
                }
                let resource = self
                    .state
                    .db
                    .create_resource(api_version, kind, ns, name, admitted_with_annot)
                    .await?;
                let context = if ns.is_some() {
                    "namespaced_apply_create"
                } else {
                    "cluster_apply_create"
                };
                reconcile_owner_refs_after_mutation(self.state, &resource, context).await;
                crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
                    self.state,
                    api_version,
                    kind,
                )
                .await;
                dispatch_generated_mutation_event(
                    self.state,
                    MutationOperation::Create,
                    &resource.data,
                    context,
                )
                .await;
                let data = inject_resource_version(resource.data, resource.resource_version);
                maybe_reconcile_cluster_role_aggregation(self.state, api_version, kind).await;
                return Ok(WriteResult::Response {
                    status: StatusCode::CREATED,
                    body: data,
                });
            }
        }

        let max_retries = 20;
        for attempt in 0..max_retries {
            let current = self
                .state
                .db
                .get_resource(api_version, kind, ns, name)
                .await?
                .ok_or_else(|| AppError::NotFound(format!("{} not found", kind)))?;

            let mut patched = if is_apply {
                crate::api::server_side_apply::server_side_apply(
                    Some(&current.data),
                    &patch,
                    &apply_manager,
                    api_version,
                    &crate::utils::k8s_time_now(),
                    apply_force,
                )
                .map_err(|conflicts| AppError::Conflict(conflicts.message()))?
            } else {
                let merged = apply_patch(&current.data, &patch, content_type)?;
                check_field_validation_strict_typed(api_version, kind, self.query, &merged)?;
                merged
            };

            prepare_admissionregistration_resource(kind, &mut patched)?;

            crate::api::rbac_admission::enforce_rbac_write_authorization(
                self.state,
                self.identity,
                api_version,
                kind,
                ns,
                &patched,
            )
            .await?;

            patched = crate::api::mutation::write::run_admission(
                self.state.db.as_ref(),
                AdmissionContextRequest {
                    api_version,
                    kind,
                    operation: "UPDATE",
                    namespace: ns.map(str::to_string),
                    name: Some(name.to_string()),
                    object: patched,
                    old_object: Some((*current.data).clone()),
                    dry_run: is_dry_run,
                    subresource: None,
                    options: None,
                },
            )
            .await?;

            if (kind == "ConfigMap" || kind == "Secret")
                && current
                    .data
                    .get("immutable")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            {
                let ns_str = ns.unwrap_or("");
                check_immutable_fields(&current.data, &patched, kind, ns_str, name)?;
            }

            if kind == "Pod"
                && let Some(namespace) = ns
            {
                validate_pod_resource_requirements_immutable(&current.data, &patched)?;
                check_resource_quota_for_pod_update(
                    self.state.db.as_ref(),
                    namespace,
                    &current.data,
                    &patched,
                )
                .await?;
            }
            if kind == "PersistentVolumeClaim"
                && let Some(namespace) = ns
            {
                check_resource_quota_for_pvc_update(
                    self.state.db.as_ref(),
                    namespace,
                    &current.data,
                    &patched,
                )
                .await?;
            }

            if kind == "PriorityClass" {
                validate_priorityclass_update_immutable(&current.data, &patched)?;
            }

            validate_builtin_resource_spec(kind, &patched)?;

            if kind == "Secret" {
                if let Err(err_msg) = validate_secret_data(&patched) {
                    return Err(AppError::UnprocessableEntity(err_msg));
                }
                process_secret_stringdata(&mut patched);
            }

            crate::api::mutation::write::prepare_builtin_generation_for_update(
                kind,
                &current.data,
                &mut patched,
            );

            preserve_status_subresource_on_main_update(
                api_version,
                kind,
                &current.data,
                &mut patched,
            );
            crate::api::finalizer_delete::preserve_deletion_timestamp_on_update(
                &current.data,
                &mut patched,
            );
            normalize_resource_for_storage(api_version, kind, &mut patched);

            if is_dry_run {
                return Ok(WriteResult::DryRun(patched));
            }

            match self
                .state
                .db
                .update_resource(
                    api_version,
                    kind,
                    ns,
                    name,
                    patched.clone(),
                    current.resource_version,
                )
                .await
            {
                Ok(resource) => return Ok(WriteResult::Persisted(resource)),
                Err(e)
                    if attempt < max_retries - 1
                        && crate::datastore::errors::is_conflict_error(&e) =>
                {
                    tracing::debug!(
                        "PATCH {}/{:?} {}: conflict on attempt {}, retrying",
                        kind,
                        ns,
                        name,
                        attempt
                    );
                    let backoff_ms = std::cmp::min(20u64.saturating_mul(1u64 << attempt), 250);
                    let _ = self
                        .state
                        .task_supervisor
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
}

pub async fn create_inner(
    state: Arc<AppState>,
    identity: &crate::auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    ns: Option<&str>,
    query: CreateUpdateQuery,
    body: Value,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let strategy = BuiltinCreateStrategy {
        state: &state,
        identity,
        api_version,
        kind,
        namespace: ns,
        query: &query,
    };
    let result = create_with_strategy(&strategy, body, dry_run).await?;
    match result {
        WriteResult::Persisted(resource) => {
            if api_version == "v1" && kind == "Pod" && ns.is_some() {
                dispatch_generated_mutation_event(
                    &state,
                    MutationOperation::Create,
                    &resource.data,
                    "pod_create",
                )
                .await;
                let data = inject_resource_version(resource.data, resource.resource_version);
                Ok((StatusCode::CREATED, Json(data)))
            } else {
                let resource_name = resource.name.clone();
                let context = if ns.is_some() {
                    "namespaced_create"
                } else {
                    "cluster_create"
                };
                reconcile_owner_refs_after_mutation(&state, &resource, context).await;
                crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
                    &state,
                    api_version,
                    kind,
                )
                .await;

                if kind == "Namespace" {
                    if let Err(e) = crate::controllers::namespace::create_default_service_account(
                        state.db.as_ref(),
                        &resource_name,
                    )
                    .await
                    {
                        tracing::warn!(
                            "Failed to create default ServiceAccount in namespace {}: {:#}",
                            resource_name,
                            e
                        );
                    }

                    let ca_cert_path =
                        crate::paths::ca_cert_path(&state.config.containerd_namespace);
                    if let Ok(ca_cert_pem) =
                        crate::utils::read_utf8_file_async(&state.file_process, &ca_cert_path).await
                    {
                        if let Err(e) =
                            crate::controllers::namespace::create_kube_root_ca_configmap(
                                state.db.as_ref(),
                                &resource_name,
                                &ca_cert_pem,
                            )
                            .await
                        {
                            tracing::warn!(
                                "Failed to create kube-root-ca.crt ConfigMap in namespace {}: {:#}",
                                resource_name,
                                e
                            );
                        }
                    } else {
                        tracing::warn!(
                            "Failed to read CA cert from {} for kube-root-ca.crt ConfigMap",
                            ca_cert_path.display()
                        );
                    }
                }

                dispatch_generated_mutation_event(
                    &state,
                    MutationOperation::Create,
                    &resource.data,
                    context,
                )
                .await;

                let data = inject_resource_version(resource.data, resource.resource_version);
                enqueue_generated_controller_after_mutation(&state, api_version, kind, &data).await;
                maybe_reconcile_cluster_role_aggregation(&state, api_version, kind).await;
                Ok((StatusCode::CREATED, Json(data)))
            }
        }
        WriteResult::DryRun(value) => Ok((StatusCode::CREATED, Json(value))),
        _ => unreachable!("create strategy returned unexpected WriteResult variant"),
    }
}

pub async fn update_inner(
    state: Arc<AppState>,
    identity: &crate::auth::AuthenticatedIdentity,
    request: GeneratedUpdateInnerRequest<'_>,
) -> Result<Json<Value>, AppError> {
    let GeneratedUpdateInnerRequest {
        target,
        query,
        body,
    } = request;
    let GeneratedNamedResource {
        api_version,
        kind,
        namespace: ns,
        name,
    } = target;
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let strategy = BuiltinUpdateStrategy {
        state: &state,
        identity,
        api_version,
        kind,
        namespace: ns,
        name,
        query: &query,
    };
    let result = update_with_strategy(&strategy, body, dry_run).await?;
    match result {
        WriteResult::DryRun(value) => Ok(Json(value)),
        WriteResult::Persisted(resource) => {
            if kind == "Pod" {
                if let Some(namespace) = ns {
                    maybe_hard_delete_pod_after_finalizers_drained(
                        state.db.as_ref(),
                        api_version,
                        kind,
                        namespace,
                        name,
                        &resource.data,
                    )
                    .await;
                }
            } else {
                crate::api::finalizer_delete::finalize_after_update_if_ready(
                    &state,
                    api_version,
                    kind,
                    ns,
                    name,
                    &resource,
                )
                .await;
            }

            let context = if ns.is_some() {
                "namespaced_update"
            } else {
                "cluster_update"
            };
            reconcile_owner_refs_after_mutation(&state, &resource, context).await;
            crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
                &state,
                api_version,
                kind,
            )
            .await;

            dispatch_generated_mutation_event(
                &state,
                MutationOperation::Update,
                &resource.data,
                context,
            )
            .await;

            // Reconcile kube-root-ca.crt if the ca.crt data was cleared or modified.
            // The K8s conformance test clears the data and expects the control
            // plane to restore it. We write the correct data back into the
            // existing ConfigMap.
            if kind == "ConfigMap"
                && name == "kube-root-ca.crt"
                && let Some(namespace) = ns
            {
                let ca_crt_empty = resource
                    .data
                    .pointer("/data/ca.crt")
                    .and_then(|v| v.as_str())
                    .is_none_or(|s| s.is_empty());
                if ca_crt_empty
                    && let Err(e) = crate::controllers::namespace::reconcile_kube_root_ca_data(
                        &state.file_process,
                        state.db.as_ref(),
                        namespace,
                    )
                    .await
                {
                    tracing::warn!(
                        namespace = %namespace,
                        error = %e,
                        "failed to reconcile kube-root-ca.crt after data modification"
                    );
                }
            }

            let data = inject_resource_version(resource.data, resource.resource_version);
            if !(api_version == "v1" && kind == "Service") {
                enqueue_generated_controller_after_mutation(&state, api_version, kind, &data).await;
            }
            maybe_reconcile_cluster_role_aggregation(&state, api_version, kind).await;
            Ok(Json(data))
        }
        _ => unreachable!("update strategy returned unexpected WriteResult variant"),
    }
}

fn metadata_resource_version(body: &Value) -> Option<i64> {
    body.pointer("/metadata/resourceVersion")
        .and_then(|value| value.as_str())
        .and_then(|value| value.parse::<i64>().ok())
}

/// bug-grpc Pillar C: durable, self-extinguishing owner-cascade sweep loop.
///
/// Replaces the former best-effort single 200 ms second pass after an owner's
/// background delete. K8s processes a dependent's ownerReferences on the
/// dependent's own events, so a child created in the cascade-vs-create race
/// window can be missed by the owner's one-shot cascade. This loop re-runs
/// `owner_cascade_sweep_once` on a capped backoff, re-enumerating dependents
/// each time, until no owned child remains non-terminating, then returns
/// (idle-silent — an idle cluster holds no sweep). Pod deletes route
/// exclusively through the actor-owned `GcPodDeleteSink`; this never
/// hard-deletes a Pod row (HR#11).
#[allow(clippy::too_many_arguments)]
async fn run_owner_cascade_sweeps(
    db: crate::datastore::DatastoreHandle,
    pod_repository: Arc<crate::kubelet::pod_repository::PodRepository>,
    supervisor: Arc<klights_supervisor::TaskSupervisor>,
    metrics: Arc<crate::side_effects::SideEffectMetrics>,
    api_version: String,
    owner_uid: String,
    owner_name: String,
    owner_kind: String,
    namespace: Option<String>,
) {
    const MAX_SWEEPS: u32 = 30;
    for attempt in 0..MAX_SWEEPS {
        let backoff_ms = std::cmp::min(200u64.saturating_mul(1u64 << attempt.min(5)), 5_000);
        if supervisor
            .sleep(
                "owner_cascade_sweep_backoff",
                std::time::Duration::from_millis(backoff_ms),
            )
            .await
            .is_err()
        {
            return; // root shutdown
        }
        match controllers::gc::owner_cascade_sweep_once(
            db.as_ref(),
            &owner_uid,
            &api_version,
            &owner_name,
            &owner_kind,
            namespace.clone(),
            pod_repository.as_ref() as &dyn klights_reconcile_api::GcPodDeleteSink,
        )
        .await
        {
            // All owned dependents are terminating or gone: self-extinguish.
            Ok(false) => return,
            // A late-created dependent was marked this sweep; keep sweeping
            // until the owner has no non-terminating children left.
            Ok(true) => continue,
            Err(e) => {
                metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(
                    namespace = ?namespace,
                    name = %owner_name,
                    error = %e,
                    "owner cascade sweep failed"
                );
            }
        }
    }
}

pub async fn delete_inner(
    state: Arc<AppState>,
    _identity: &crate::auth::AuthenticatedIdentity,
    request: GeneratedDeleteInnerRequest<'_>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let GeneratedDeleteInnerRequest {
        target,
        query,
        body,
    } = request;
    let GeneratedNamedResource {
        api_version,
        kind,
        namespace: ns,
        name,
    } = target;
    let delete_intent = crate::api::mutation::DeleteIntent::from_query_and_body(&query, &body)?;
    let is_dry_run = delete_intent.dry_run.is_all();

    let resource = state
        .db
        .get_resource(api_version, kind, ns, name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("{} not found", kind)))?;
    crate::api::mutation::delete::ensure_delete_preconditions_match(
        &resource,
        &delete_intent.preconditions,
    )?;

    let delete_options_value =
        serde_json::to_value(&delete_intent.options).unwrap_or_else(|_| serde_json::json!({}));
    let _ = crate::api::mutation::write::run_admission(
        state.db.as_ref(),
        AdmissionContextRequest {
            api_version,
            kind,
            operation: "DELETE",
            namespace: ns.map(str::to_string),
            name: Some(name.to_string()),
            object: Value::Null,
            old_object: Some((*resource.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: Some(delete_options_value),
        },
    )
    .await?;

    if kind == "Pod"
        && let Some(namespace) = ns
    {
        let outcome = crate::kubelet::pod_repository::PodApiWriter::api_delete_pod(
            state.pod_repository.as_ref(),
            namespace,
            name,
            delete_intent.options,
            is_dry_run,
        )
        .await?;
        return match outcome {
            crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(v) => {
                Ok((StatusCode::OK, Json(v)))
            }
            crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(r) => {
                // Fire side effects (ResourceQuota recount, etc.) after
                // pod deletionTimestamp is set. The pod still exists in the
                // datastore but the RQ reconciler excludes terminating pods.
                tracing::info!(
                    kind = %r.data.get("kind").and_then(|v| v.as_str()).unwrap_or("?"),
                    name = %r.name,
                    namespace = %r.data.pointer("/metadata/namespace").and_then(|v| v.as_str()).unwrap_or("?"),
                    "pod delete GracefulSet: firing side effects"
                );
                dispatch_generated_mutation_event(
                    &state,
                    klights_reconcile_api::MutationOperation::DeleteMark,
                    &r.data,
                    "pod_delete_mark",
                )
                .await;
                Ok((
                    StatusCode::ACCEPTED,
                    Json(crate::api::mutation::response::accepted_object(
                        r.data,
                        r.resource_version,
                    )),
                ))
            }
        };
    }

    if is_dry_run {
        let mut del_data: Value = (*resource.data).clone();
        set_deletion_timestamp(&mut del_data);
        let result =
            crate::api::mutation::response::persisted_object(del_data, resource.resource_version);
        return Ok((StatusCode::OK, Json(result)));
    }

    let target_identity =
        klights_types::ResourceKey::new(api_version, kind, ns.map(str::to_string), name);
    let delete_strategy = crate::api::mutation::delete::FinalizerAwareDeleteStrategy {
        db: state.db.as_ref(),
    };
    let outcome = crate::api::mutation::delete::delete_loaded_with_strategy(
        &delete_strategy,
        target_identity,
        resource,
        &delete_intent,
    )
    .await?;
    let resource = match outcome {
        crate::api::mutation::delete::DeleteResult::MarkedTerminating(updated) => {
            schedule_foreground_owner_finalization(
                &state,
                api_version,
                kind,
                ns,
                name,
                updated.clone(),
            )
            .await;

            crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
                &state,
                api_version,
                kind,
            )
            .await;
            maybe_reconcile_cluster_role_aggregation(&state, api_version, kind).await;
            let data = crate::api::mutation::response::accepted_object(
                updated.data,
                updated.resource_version,
            );
            return Ok((StatusCode::ACCEPTED, Json(data)));
        }
        crate::api::mutation::delete::DeleteResult::GoneOrUidChanged => {
            return Err(AppError::NotFound(format!("{} not found", kind)));
        }
        crate::api::mutation::delete::DeleteResult::HardDeleted(resource) => resource,
    };

    let owner_name_gc = resource.name.clone();
    let owner_kind_gc = kind.to_string();
    let owner_uid = resource.uid.clone();

    run_post_hard_delete_effects(&state, api_version, kind, ns, &resource, false).await;
    if api_version == "v1"
        && kind == "Node"
        && let Err(err) = state
            .db
            .delete_pod_cleanup_intents_for_node(&resource.name)
            .await
    {
        tracing::warn!(
            node = %resource.name,
            error = %err,
            "failed to delete pod cleanup intents for deleted node"
        );
    }

    if !delete_intent.orphan_children {
        if let Err(e) = controllers::gc::cascade_delete_with_uid(
            state.db.as_ref(),
            &owner_uid,
            api_version,
            &owner_name_gc,
            &owner_kind_gc,
            ns.map(str::to_string),
            state.pod_repository.as_ref() as &dyn klights_reconcile_api::GcPodDeleteSink,
        )
        .await
        {
            state
                .metrics
                .cascade_delete_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::error!(namespace = ?ns, name = %owner_name_gc, error = %e, "cascade delete failed");
        }

        // bug-grpc Pillar C: durable, self-extinguishing owner-cascade sweep.
        // Replaces the former best-effort single 200 ms second pass. The sweep
        // re-enumerates the owner's dependents on a backoff, so a child Pod
        // created in the cascade-vs-create race window (the EmptyDir-wrapper
        // survivor) is still marked terminating and routed to actor-owned
        // finalization, and the loop stops as soon as no owned child remains
        // non-terminating (idle-silent).
        if let Err(err) = state
            .task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                "owner_cascade_sweeps",
                run_owner_cascade_sweeps(
                    state.db.clone(),
                    state.pod_repository.clone(),
                    state.task_supervisor.clone(),
                    state.metrics.clone(),
                    api_version.to_string(),
                    owner_uid.clone(),
                    owner_name_gc.clone(),
                    owner_kind_gc.clone(),
                    ns.map(str::to_string),
                ),
            )
            .await
        {
            tracing::warn!("Failed to schedule owner cascade sweep: {}", err);
        }
    }

    // Recreate kube-root-ca.crt if deleted. Termination check is inside
    // the reconcile function itself.
    if kind == "ConfigMap"
        && name == "kube-root-ca.crt"
        && let Some(namespace) = ns
        && let Err(e) = crate::controllers::namespace::reconcile_kube_root_ca(
            &state.file_process,
            state.db.as_ref(),
            namespace,
        )
        .await
    {
        tracing::warn!(
            namespace = %namespace,
            error = %e,
            "failed to recreate kube-root-ca.crt after deletion"
        );
    }

    if kind == "EndpointSlice"
        && let Some(namespace) = ns
    {
        maybe_reconcile_service_after_controller_endpointslice_delete(
            &state,
            namespace,
            &resource.data,
        )
        .await?;
    }

    // Endpoints' normal hook is mirror-upsert; after hard delete the
    // endpoint-mirror delete hook above is authoritative.
    if kind != "ResourceQuota" && kind != "Endpoints" {
        dispatch_generated_mutation_event(
            &state,
            klights_reconcile_api::MutationOperation::DeleteMark,
            &resource.data,
            "generated_delete_reconcile",
        )
        .await;
    }

    let data =
        crate::api::mutation::response::persisted_object(resource.data, resource.resource_version);
    maybe_reconcile_cluster_role_aggregation(&state, api_version, kind).await;
    Ok((StatusCode::OK, Json(data)))
}

pub async fn patch_inner(
    state: Arc<AppState>,
    identity: &crate::auth::AuthenticatedIdentity,
    request: GeneratedPatchInnerRequest<'_>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let GeneratedPatchInnerRequest {
        target,
        query,
        headers,
        body,
    } = request;
    let GeneratedNamedResource {
        api_version,
        kind,
        namespace: ns,
        name,
    } = target;
    check_content_type(&headers)?;

    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());

    let patch: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        klights_kube_protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))?
    } else if content_type == Some("application/apply-patch+yaml") {
        parse_apply_yaml(&body)?
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))?
    };

    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let strategy = BuiltinPatchStrategy {
        state: &state,
        identity,
        target: GeneratedNamedResource::new(api_version, kind, ns, name),
        query: &query,
        headers: &headers,
    };
    let result = patch_with_strategy(&strategy, patch, dry_run).await?;
    let result = match result {
        WriteResult::Persisted(resource) => {
            if kind == "Pod" {
                if let Some(namespace) = ns {
                    maybe_hard_delete_pod_after_finalizers_drained(
                        state.db.as_ref(),
                        api_version,
                        kind,
                        namespace,
                        name,
                        &resource.data,
                    )
                    .await;
                }
            } else {
                crate::api::finalizer_delete::finalize_after_update_if_ready(
                    &state,
                    api_version,
                    kind,
                    ns,
                    name,
                    &resource,
                )
                .await;
            }

            let context = if ns.is_some() {
                "namespaced_patch"
            } else {
                "cluster_patch"
            };
            reconcile_owner_refs_after_mutation(&state, &resource, context).await;
            crate::api::apiservice_proxy::invalidate_apiservice_proxy_cache_for_resource(
                &state,
                api_version,
                kind,
            )
            .await;

            dispatch_generated_mutation_event(
                &state,
                MutationOperation::Patch,
                &resource.data,
                context,
            )
            .await;

            let data = inject_resource_version(resource.data.clone(), resource.resource_version);
            if !(api_version == "v1" && kind == "Service") {
                enqueue_generated_controller_after_mutation(&state, api_version, kind, &data).await;
            }
            maybe_reconcile_cluster_role_aggregation(&state, api_version, kind).await;
            WriteResult::Persisted(resource)
        }
        other => other,
    };
    let (status, data) = result.into_response_parts(StatusCode::OK);
    Ok((status, Json(data)))
}

pub async fn delete_collection_inner(
    state: Arc<AppState>,
    identity: &crate::auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: &str,
    query: DeleteCollectionQuery,
) -> Result<Json<Value>, AppError> {
    delete_collection_shared_inner(state, identity, api_version, kind, Some(namespace), query).await
}

pub async fn delete_collection_shared_inner(
    state: Arc<AppState>,
    _identity: &crate::auth::AuthenticatedIdentity,
    api_version: &'static str,
    kind: &'static str,
    namespace: Option<&str>,
    query: DeleteCollectionQuery,
) -> Result<Json<Value>, AppError> {
    let dry_run = crate::api::mutation::DryRunMode::from_delete_collection_query(&query)?;
    let is_dry_run = dry_run.is_all();
    let list = state
        .db
        .list_resources(
            api_version,
            kind,
            namespace,
            crate::datastore::ResourceListQuery::new(
                query.label_selector.as_deref(),
                None,
                None,
                None,
            ),
        )
        .await?;

    if is_dry_run {
        return Ok(Json(
            crate::api::mutation::response::delete_collection_success_status(),
        ));
    }

    let delete_strategy = crate::api::mutation::delete::FinalizerAwareDeleteStrategy {
        db: state.db.as_ref(),
    };

    for resource in list.items {
        let owner_uid = resource.uid.clone();
        let res_name = resource.name.clone();
        let target_identity = klights_types::ResourceKey::new(
            api_version,
            kind,
            namespace.map(str::to_string),
            res_name.clone(),
        );
        let item_intent = crate::api::mutation::DeleteIntent::collection_item(
            dry_run,
            crate::datastore::ResourcePreconditions::uid(owner_uid.clone()),
        );
        match crate::api::mutation::delete::delete_loaded_with_strategy(
            &delete_strategy,
            target_identity,
            resource,
            &item_intent,
        )
        .await
        {
            Ok(crate::api::mutation::delete::DeleteResult::HardDeleted(deleted)) => {
                run_post_hard_delete_effects(&state, api_version, kind, namespace, &deleted, false)
                    .await;
                if let Err(e) = controllers::gc::cascade_delete_with_uid(
                    state.db.as_ref(),
                    &owner_uid,
                    api_version,
                    &res_name,
                    kind,
                    namespace.map(str::to_string),
                    state.pod_repository.as_ref() as &dyn klights_reconcile_api::GcPodDeleteSink,
                )
                .await
                {
                    state
                        .metrics
                        .cascade_delete_failures_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::error!(namespace = ?namespace, name = %res_name, error = %e, "delete collection: cascade delete failed");
                }
            }
            Ok(crate::api::mutation::delete::DeleteResult::MarkedTerminating(_))
            | Ok(crate::api::mutation::delete::DeleteResult::GoneOrUidChanged) => {}
            Err(e) => {
                state
                    .metrics
                    .cascade_delete_failures_total
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::error!(namespace = ?namespace, name = %res_name, error = ?e, "delete collection: resource delete failed");
            }
        }
    }

    // Endpoints' normal hook is mirror-upsert; deletecollection already ran
    // the endpoint-mirror delete hook for each hard-deleted row above.
    if kind != "ResourceQuota" && kind != "Endpoints" {
        let metadata = namespace
            .map(|namespace| serde_json::json!({"namespace": namespace}))
            .unwrap_or_else(|| serde_json::json!({}));
        let stub = serde_json::json!({
            "apiVersion": api_version,
            "kind": kind,
            "metadata": metadata,
        });
        dispatch_generated_mutation_event(
            &state,
            klights_reconcile_api::MutationOperation::DeleteMark,
            &stub,
            "generated_delete_collection",
        )
        .await;
    }

    maybe_reconcile_cluster_role_aggregation(&state, api_version, kind).await;

    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;
    use std::sync::Arc;

    fn default_create_update_query() -> CreateUpdateQuery {
        CreateUpdateQuery {
            dry_run: None,
            field_manager: None,
            field_validation: None,
            force: None,
            orphan_dependents: None,
            propagation_policy: None,
            grace_period_seconds: None,
        }
    }

    fn aggregate_widgets_rule() -> Value {
        json!({
            "verbs": ["get", "list"],
            "apiGroups": ["example.klights.io"],
            "resources": ["widgets"]
        })
    }

    async fn seeded_rbac_state() -> Arc<AppState> {
        let state = Arc::new(crate::api::test_support::build_test_app_state().await);
        crate::controllers::rbac_reconcile::reconcile_default_rbac_objects(state.db.as_ref())
            .await
            .expect("seed default RBAC");
        state
    }

    async fn create_labeled_aggregate_source(state: &Arc<AppState>, name: &str, rule: Value) {
        state
            .db
            .create_resource(
                "rbac.authorization.k8s.io/v1",
                "ClusterRole",
                None,
                name,
                json!({
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "metadata": {
                        "name": name,
                        "labels": {"rbac.authorization.k8s.io/aggregate-to-view": "true"}
                    },
                    "rules": [rule]
                }),
            )
            .await
            .expect("create aggregate source");
        crate::controllers::rbac_reconcile::reconcile_cluster_role_aggregation(state.db.as_ref())
            .await
            .expect("seed aggregate rules");
    }

    async fn view_has_rule(state: &Arc<AppState>, expected: &Value) -> bool {
        let view = state
            .db
            .get_resource("rbac.authorization.k8s.io/v1", "ClusterRole", None, "view")
            .await
            .expect("read view")
            .expect("view ClusterRole exists");
        view.data
            .get("rules")
            .and_then(Value::as_array)
            .expect("view should have rules")
            .iter()
            .any(|rule| rule == expected)
    }

    fn kubelet_client_csr_b64(node_name: &str) -> String {
        use rcgen::{CertificateParams, DnType, KeyPair};

        let mut params = CertificateParams::default();
        params.distinguished_name = rcgen::DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, format!("system:node:{node_name}"));
        params
            .distinguished_name
            .push(DnType::OrganizationName, "system:nodes".to_string());
        let key_pair = KeyPair::generate().expect("test keypair");
        let csr_pem = params
            .serialize_request(&key_pair)
            .expect("test CSR")
            .pem()
            .expect("CSR PEM");

        base64::engine::general_purpose::STANDARD.encode(csr_pem.as_bytes())
    }

    #[tokio::test]
    async fn create_certificate_signing_request_dispatches_csr_signer() {
        let mut state = crate::api::test_support::build_test_app_state().await;
        let signer = Arc::new(crate::auth::csr_signer::RecordingCsrSigner::new());
        let dispatcher = Arc::new(
            crate::controller_dispatcher::ControllerDispatcher::new_with_nodeport(
                state.service_ipam.clone(),
                state.nodeport_alloc.clone(),
                state.task_supervisor.clone(),
                Some(signer.clone()),
            ),
        );
        dispatcher
            .set_sync_context(state.db.clone(), state.config.node_name.clone())
            .await;
        dispatcher
            .set_pod_repository(state.pod_repository.clone())
            .await;
        state.controller_dispatcher = dispatcher;
        let state = Arc::new(state);

        let body = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "node-bootstrap-csr"},
            "spec": {
                "request": kubelet_client_csr_b64("mn-worker"),
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"]
            }
        });
        let identity = crate::auth::AuthenticatedIdentity::bootstrap(
            "abcdef",
            &["system:bootstrappers:klights:worker".to_string()],
        );

        let (status, _) = create_inner(
            state.clone(),
            &identity,
            "certificates.k8s.io/v1",
            "CertificateSigningRequest",
            None,
            CreateUpdateQuery {
                dry_run: None,
                field_manager: None,
                field_validation: None,
                force: None,
                orphan_dependents: None,
                propagation_policy: None,
                grace_period_seconds: None,
            },
            body,
        )
        .await
        .expect("create CSR");

        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(
            signer.request_count(),
            1,
            "CSR create must enqueue CsrSignerController"
        );

        let stored = state
            .db
            .get_resource(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                None,
                "node-bootstrap-csr",
            )
            .await
            .expect("read CSR")
            .expect("CSR exists");
        assert!(
            stored.data.pointer("/status/certificate").is_some(),
            "CSR signer must write status.certificate after API create"
        );
    }

    #[tokio::test]
    async fn apply_create_csr_cannot_forge_spec_identity() {
        // Server-side-apply create of a CSR must stamp spec identity from the
        // authenticated caller, exactly like POST create. Otherwise the
        // auto-signer would trust a forged spec.username/groups and mint a cert
        // for another node's identity.
        let state = Arc::new(crate::api::test_support::build_test_app_state().await);

        let forged = json!({
            "apiVersion": "certificates.k8s.io/v1",
            "kind": "CertificateSigningRequest",
            "metadata": {"name": "apply-forge-csr"},
            "spec": {
                "request": kubelet_client_csr_b64("victim"),
                "signerName": "kubernetes.io/kube-apiserver-client-kubelet",
                "usages": ["client auth"],
                "username": "system:node:victim",
                "groups": ["system:nodes"],
                "uid": "forged-uid"
            }
        });

        let mut headers = HeaderMap::new();
        headers.insert(
            "content-type",
            "application/apply-patch+yaml".parse().unwrap(),
        );

        let identity = crate::auth::AuthenticatedIdentity::bootstrap(
            "abcdef",
            &["system:bootstrappers:klights:worker".to_string()],
        );

        let _ = patch_inner(
            state.clone(),
            &identity,
            GeneratedPatchInnerRequest {
                target: GeneratedNamedResource {
                    api_version: "certificates.k8s.io/v1",
                    kind: "CertificateSigningRequest",
                    namespace: None,
                    name: "apply-forge-csr",
                },
                query: CreateUpdateQuery {
                    dry_run: None,
                    field_manager: None,
                    field_validation: None,
                    force: None,
                    orphan_dependents: None,
                    propagation_policy: None,
                    grace_period_seconds: None,
                },
                headers,
                body: Bytes::from(serde_json::to_vec(&forged).unwrap()),
            },
        )
        .await
        .expect("apply-create CSR");

        let stored = state
            .db
            .get_resource(
                "certificates.k8s.io/v1",
                "CertificateSigningRequest",
                None,
                "apply-forge-csr",
            )
            .await
            .expect("read CSR")
            .expect("CSR exists");

        assert_eq!(
            stored
                .data
                .pointer("/spec/username")
                .and_then(|v| v.as_str()),
            Some(identity.username.as_str()),
            "apply-create must stamp the authenticated username, not the forged one"
        );
        let groups = stored
            .data
            .pointer("/spec/groups")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        assert!(
            !groups.iter().any(|g| g.as_str() == Some("system:nodes")),
            "forged system:nodes group must not be persisted"
        );
    }

    #[tokio::test]
    async fn cluster_role_create_reconciles_aggregation_immediately() {
        let state = seeded_rbac_state().await;
        let aggregate_rule = aggregate_widgets_rule();
        let body = json!({
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRole",
            "metadata": {
                "name": "aggregate-widgets-view",
                "labels": {"rbac.authorization.k8s.io/aggregate-to-view": "true"}
            },
            "rules": [aggregate_rule.clone()]
        });
        let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");

        let (status, _) = create_inner(
            state.clone(),
            &identity,
            "rbac.authorization.k8s.io/v1",
            "ClusterRole",
            None,
            default_create_update_query(),
            body,
        )
        .await
        .expect("create aggregating ClusterRole");

        assert_eq!(status, StatusCode::CREATED);

        assert!(
            view_has_rule(&state, &aggregate_rule).await,
            "live ClusterRole create should reconcile aggregate-to-view rules"
        );
    }

    #[tokio::test]
    async fn cluster_role_update_reconciles_aggregation_label_removal_immediately() {
        let state = seeded_rbac_state().await;
        let aggregate_rule = aggregate_widgets_rule();
        create_labeled_aggregate_source(&state, "aggregate-widgets-view", aggregate_rule.clone())
            .await;
        assert!(view_has_rule(&state, &aggregate_rule).await);

        let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
        let _ = update_inner(
            state.clone(),
            &identity,
            GeneratedUpdateInnerRequest {
                target: GeneratedNamedResource {
                    api_version: "rbac.authorization.k8s.io/v1",
                    kind: "ClusterRole",
                    namespace: None,
                    name: "aggregate-widgets-view",
                },
                query: default_create_update_query(),
                body: json!({
                    "apiVersion": "rbac.authorization.k8s.io/v1",
                    "kind": "ClusterRole",
                    "metadata": {"name": "aggregate-widgets-view"},
                    "rules": [aggregate_rule.clone()]
                }),
            },
        )
        .await
        .expect("remove aggregate label");

        assert!(
            !view_has_rule(&state, &aggregate_rule).await,
            "live ClusterRole update should revoke rules when aggregate label is removed"
        );
    }

    #[tokio::test]
    async fn cluster_role_delete_reconciles_aggregation_immediately() {
        let state = seeded_rbac_state().await;
        let aggregate_rule = aggregate_widgets_rule();
        create_labeled_aggregate_source(&state, "aggregate-widgets-view", aggregate_rule.clone())
            .await;
        assert!(view_has_rule(&state, &aggregate_rule).await);

        let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
        let _ = delete_inner(
            state.clone(),
            &identity,
            GeneratedDeleteInnerRequest {
                target: GeneratedNamedResource {
                    api_version: "rbac.authorization.k8s.io/v1",
                    kind: "ClusterRole",
                    namespace: None,
                    name: "aggregate-widgets-view",
                },
                query: default_create_update_query(),
                body: Bytes::new(),
            },
        )
        .await
        .expect("delete aggregate source");

        assert!(
            !view_has_rule(&state, &aggregate_rule).await,
            "live ClusterRole delete should revoke aggregated source rules"
        );
    }

    #[tokio::test]
    async fn foreground_delete_returns_after_marking_owner_without_synchronous_pod_cascade() {
        let mut app_state = crate::api::test_support::build_test_app_state().await;
        let release_workqueue = Arc::new(tokio::sync::Notify::new());
        let task_supervisor = Arc::new(klights_supervisor::TaskSupervisor::new(
            klights_supervisor::TaskCategoryConfig {
                pod_delete_workqueue: 1,
                ..klights_supervisor::TaskCategoryConfig::default()
            },
        ));
        let held_workqueue = task_supervisor
            .spawn_async(
                klights_supervisor::TaskCategory::PodDeleteWorkqueue,
                "hold_foreground_delete_workqueue_for_test",
                {
                    let release_workqueue = release_workqueue.clone();
                    async move {
                        release_workqueue.notified().await;
                    }
                },
            )
            .await
            .expect("hold pod-delete workqueue permit");
        app_state.task_supervisor = task_supervisor;
        let state = Arc::new(app_state);
        let owner_uid = "fg-rc-owner-uid";
        state
            .db
            .create_resource(
                "v1",
                "ReplicationController",
                Some("default"),
                "fg-rc",
                json!({
                    "apiVersion": "v1",
                    "kind": "ReplicationController",
                    "metadata": {
                        "name": "fg-rc",
                        "namespace": "default",
                        "uid": owner_uid
                    }
                }),
            )
            .await
            .expect("create foreground RC");

        for i in 0..3 {
            let pod_name = format!("fg-rc-pod-{i}");
            let pod_uid = format!("fg-rc-pod-{i}-uid");
            state
                .db
                .create_resource(
                    "v1",
                    "Pod",
                    Some("default"),
                    &pod_name,
                    json!({
                        "apiVersion": "v1",
                        "kind": "Pod",
                        "metadata": {
                            "name": pod_name,
                            "namespace": "default",
                            "uid": pod_uid,
                            "ownerReferences": [{
                                "apiVersion": "v1",
                                "kind": "ReplicationController",
                                "name": "fg-rc",
                                "uid": owner_uid,
                                "blockOwnerDeletion": true
                            }]
                        },
                        "spec": {
                            "containers": [{"name": "nginx", "image": "nginx"}]
                        }
                    }),
                )
                .await
                .expect("create RC child Pod");
        }

        let identity = crate::auth::AuthenticatedIdentity::admin("test-admin");
        let (status, body) = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            delete_inner(
                state.clone(),
                &identity,
                GeneratedDeleteInnerRequest {
                    target: GeneratedNamedResource {
                        api_version: "v1",
                        kind: "ReplicationController",
                        namespace: Some("default"),
                        name: "fg-rc",
                    },
                    query: CreateUpdateQuery {
                        dry_run: None,
                        field_manager: None,
                        field_validation: None,
                        force: None,
                        orphan_dependents: None,
                        propagation_policy: Some("Foreground".to_string()),
                        grace_period_seconds: None,
                    },
                    body: Bytes::new(),
                },
            ),
        )
        .await
        .expect("foreground delete response should not wait for pod-delete workqueue")
        .expect("foreground delete RC");

        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(
            body.0.pointer("/metadata/deletionTimestamp").is_some(),
            "foreground delete response must mark the owner terminating"
        );
        assert!(
            body.0
                .pointer("/metadata/finalizers")
                .and_then(Value::as_array)
                .is_some_and(|finalizers| finalizers
                    .iter()
                    .any(|finalizer| finalizer.as_str() == Some("foregroundDeletion"))),
            "foreground delete response must retain the foregroundDeletion finalizer"
        );

        for i in 0..3 {
            let pod_name = format!("fg-rc-pod-{i}");
            let pod = state
                .db
                .get_resource("v1", "Pod", Some("default"), &pod_name)
                .await
                .expect("read child Pod")
                .expect("child Pod should still exist");
            assert!(
                pod.data.pointer("/metadata/deletionTimestamp").is_none(),
                "foreground delete must not mark child Pod {pod_name} terminating before the owner DELETE response returns"
            );
        }

        release_workqueue.notify_waiters();
        held_workqueue.abort();
    }
}
