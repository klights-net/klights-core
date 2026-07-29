//! Dedicated HTTP handlers for `v1/Pod`.
//!
//! Extracted verbatim from the `namespaced_resource_handlers!` and
//! `cluster_wide_list_handler!` macros (Task 11 Step A — pure refactor, no
//! behavior change). Subsequent tasks (Step B onward) will route the create
//! path through `PodApiService::api_create_pod`; for now this file mirrors the
//! macro expansion bit-for-bit.

use crate::api::mutation::write::{
    CreateStrategy, PatchStrategy, UpdateStrategy, WriteResult, create_with_strategy,
    patch_with_strategy, update_with_strategy,
};
use crate::api::*;
use async_trait::async_trait;

async fn dispatch_pod_handler_mutation_event(
    state: &Arc<ApiState>,
    operation: klights_reconcile_api::MutationOperation,
    resource: &Value,
    context: &'static str,
) {
    crate::api::mutation::dispatch_mutation_event(
        state.resource_mutation().mutation_effects.as_ref(),
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

pub(in crate::api) async fn list_pods(
    State(state): State<Arc<ApiState>>,
    Path(namespace): Path<String>,
    Query(query): Query<ListQuery>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    validate_builtin_field_selector(
        "v1",
        "Pod",
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        true,
    )?;
    if query.watch == Some("true".to_string()) {
        query.validate_send_initial_events_watch()?;
        // Watch streaming
        let kind = "Pod".to_string();
        let ns = namespace.clone();
        let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
        let table_format = wants_table_format(&headers)?;
        let protobuf_supported = protobuf_watch_supported_for_request(
            "v1",
            "Pod",
            table_format,
            query.label_selector.as_deref(),
            query.field_selector.as_deref(),
        );
        let stream_format = negotiate_watch_stream_format(&headers, protobuf_supported)?;
        let label_selector = query.label_selector.clone();
        let field_selector = query.field_selector.clone();

        // Parse resourceVersion filter (0 or missing = send all, >0 = filter old events)
        let requested_rv: i64 = query
            .resource_version
            .as_ref()
            .and_then(|rv| rv.parse::<i64>().ok())
            .unwrap_or(0);
        let explicit_resource_version_zero = query
            .resource_version
            .as_deref()
            .is_some_and(|rv| rv.trim() == "0");

        // K8s watch semantics: default watch does NOT replay initial objects.
        // Initial list+watch replay is only enabled when sendInitialEvents=true.
        let send_initial_events = query.send_initial_events.as_deref() == Some("true");
        let watch_stream = state.resource_mutation().watch_stream.clone();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            source: watch_stream,
            task_supervisor: state.operational().task_supervisor.clone(),
            api_version: "v1",
            kind,
            watch_namespace: Some(ns),
            requested_rv,
            send_initial_events,
            send_bookmarks,
            label_selector,
            field_selector,
            table_format,
            stream_format,
            timeout_seconds: query.timeout_seconds,
            emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
            wall_clock: state.operational().clock.clone(),
        })
        .await;
        return Ok(Response::builder()
            .header("Content-Type", stream_format.content_type())
            .header("Transfer-Encoding", "chunked")
            .body(body)
            .unwrap());
    }

    let operation_now = state.operational().clock.now();
    let normalized_limit = query.normalized_limit()?;

    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|t| !t.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;

    // Decode continue token: check TTL and extract name for DB filter.
    let (db_continue_name, continue_resource_version) =
        process_continue_token_at(query.continue_token, operation_now.unix_timestamp())?;

    let list_query = klights_pod_api::PodListRequest::try_new(
        Some(namespace.clone()),
        query.label_selector.clone(),
        query.field_selector.clone(),
        normalized_limit,
        db_continue_name,
    )
    .map_err(AppError::from)?;

    // Pin paginated continuations / Exact reads to a consistent snapshot, shared
    // with every other list handler. Pods live in the generic resource table, so
    // the snapshot side reads `("v1","Pod")` directly; the live side stays on the
    // PodReader port. See `query::resolve_list_page`.
    let pod_repository = state.resource_mutation().pod_repository.clone();
    let snapshot_repository = pod_repository.clone();
    let snapshot_query = list_query.clone();
    let live_query = list_query;
    let crate::api::query::ResolvedListPage {
        list,
        response_rv,
        continue_resource_version,
    } = crate::api::query::resolve_list_page(
        state.resource_mutation().list_resource_versions.as_ref(),
        rv_match,
        continue_resource_version,
        |srv| async move {
            klights_pod_api::PodSnapshotQuery::snapshot_pods(
                snapshot_repository.as_ref(),
                klights_pod_api::PodSnapshotListRequest {
                    list: snapshot_query,
                    snapshot_resource_version: srv,
                },
            )
            .await
            .map_err(AppError::from)
        },
        || async move {
            klights_pod_api::PodQuery::list_pods(pod_repository.as_ref(), live_query)
                .await
                .map_err(AppError::from)
        },
    )
    .await?;

    let (list_items, _, list_continue_token, remaining_item_count) = list.into_parts();
    let items: Vec<Value> = list_items
        .into_iter()
        .map(|r| {
            let mut data = inject_resource_version(r.data, r.resource_version);
            normalize_resource_for_read("v1", "Pod", &mut data);
            data
        })
        .collect();
    let resource_version = response_rv.to_string();

    // Return Table format if requested by kubectl
    if wants_table_format(&headers)? {
        let table = pod_list_to_table_at(items, resource_version, operation_now);
        return Ok(Json(table).into_response());
    }

    // Return normal List format
    // Omit "continue" when None; include "remainingItemCount" only when paginating.
    let mut metadata = serde_json::json!({
        "resourceVersion": resource_version,
    });
    if let Some(ref name) = list_continue_token {
        // Normal pages keep the session RV; inconsistent recovery pages must
        // keep returning inconsistent tokens.
        let token = crate::api::query::encode_response_continue_token_at(
            name,
            response_rv,
            continue_resource_version,
            operation_now.unix_timestamp(),
        );
        metadata["continue"] = serde_json::json!(token);
    }
    if let Some(remaining) = remaining_item_count {
        metadata["remainingItemCount"] = serde_json::json!(remaining);
    }
    let response = serde_json::json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": metadata,
        "items": items,
    });

    Ok(K8sResponse::new(response, &headers).into_response())
}

pub(in crate::api) async fn get_pod(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<K8sResponse, AppError> {
    match crate::api::pod_repository_ports::get_pod(
        state.resource_mutation().pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    {
        Some(resource) => {
            let mut data = inject_resource_version(resource.data, resource.resource_version);
            normalize_resource_for_read("v1", "Pod", &mut data);
            Ok(K8sResponse::new(data, &headers))
        }
        None => Err(AppError::NotFound("Pod not found".to_string())),
    }
}

pub(in crate::api) async fn create_pod(
    State(state): State<Arc<ApiState>>,
    Path(namespace): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(body): LenientJson<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let strategy = PodCreateStrategy {
        state: &state,
        namespace: &namespace,
        query: &query,
    };
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let result = create_with_strategy(&strategy, body, dry_run).await?;
    match result {
        WriteResult::Persisted(resource) => {
            dispatch_pod_handler_mutation_event(
                &state,
                klights_reconcile_api::MutationOperation::Create,
                &resource.data,
                "pod_create",
            )
            .await;
            let data = inject_resource_version(resource.data, resource.resource_version);
            Ok((StatusCode::CREATED, Json(data)))
        }
        WriteResult::DryRun(body) | WriteResult::PersistedValue(body) => {
            Ok((StatusCode::CREATED, Json(body)))
        }
        WriteResult::Response { status, body } => Ok((status, Json(body))),
    }
}

pub(in crate::api) async fn update_pod(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    tracing::debug!(
        "UPDATE {}/{} in {}: body keys: {:?}",
        "Pod",
        name,
        namespace,
        body.as_object().map(|o| o.keys().collect::<Vec<_>>())
    );

    let strategy = PodUpdateStrategy {
        state: &state,
        namespace: &namespace,
        name: &name,
        query: &query,
    };
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let result = update_with_strategy(&strategy, body, dry_run).await?;
    let resource = match result {
        WriteResult::DryRun(b) | WriteResult::PersistedValue(b) => return Ok(Json(b)),
        WriteResult::Persisted(r) => r,
        WriteResult::Response { status: _, body } => return Ok(Json(body)),
    };

    tracing::debug!(
        "UPDATE {}/{} in {}: after db.update_resource",
        "Pod",
        name,
        namespace
    );

    reconcile_owner_refs_after_mutation(&state, &resource, "namespaced_update").await;

    dispatch_pod_handler_mutation_event(
        &state,
        klights_reconcile_api::MutationOperation::Update,
        &resource.data,
        "pod_update",
    )
    .await;

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

pub(in crate::api) async fn delete_pod(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let delete_intent = crate::api::mutation::DeleteIntent::from_query_and_body(&query, &body)?;
    // Note: propagation policy / orphanDependents are read at the macro-level
    // for non-Pod kinds. Pod delete defers cascade through PodWorkqueue, so
    // the option is captured into PodApiService once Pod delete gains an
    // explicit propagation-policy field. For now, behavior remains
    // always-cascade by not threading the policy here.
    let is_dry_run = delete_intent.dry_run.is_all();

    let outcome = klights_pod_api::PodApiMutation::delete_pod(
        state.resource_mutation().pod_repository.as_ref(),
        klights_pod_api::PodApiDeleteRequest {
            namespace,
            name,
            options: delete_intent.options.into(),
            dry_run: is_dry_run,
        },
    )
    .await?;

    match outcome {
        klights_pod_api::PodApiDeleteOutcome::DryRun(v) => Ok((StatusCode::OK, Json(v))),
        klights_pod_api::PodApiDeleteOutcome::GracefulSet(r) => {
            dispatch_pod_handler_mutation_event(
                &state,
                klights_reconcile_api::MutationOperation::DeleteMark,
                &r.data,
                "pod_delete_mark",
            )
            .await;
            let result =
                crate::api::mutation::response::accepted_object(r.data, r.resource_version);
            Ok((StatusCode::ACCEPTED, Json(result)))
        }
    }
}

pub(in crate::api) async fn patch_pod(
    State(state): State<Arc<ApiState>>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<CreateUpdateQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<Value>, AppError> {
    check_content_type(&headers)?;

    // Check content-type first to determine how to parse the body
    let content_type = headers.get("content-type").and_then(|h| h.to_str().ok());

    // Parse body based on content-type (parse once, reuse in retry loop)
    let patch: Value = if body.len() >= 4 && &body[..4] == b"k8s\x00" {
        // Protobuf encoded
        klights_kube_protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))?
    } else if content_type == Some("application/apply-patch+yaml") {
        // YAML for server-side apply
        parse_apply_yaml(&body)?
    } else {
        // Default to JSON
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))?
    };

    let strategy = PodPatchStrategy {
        state: &state,
        namespace: &namespace,
        name: &name,
        query: &query,
        headers: &headers,
    };
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let result = patch_with_strategy(&strategy, patch, dry_run).await?;
    let resource = match result {
        WriteResult::DryRun(b) | WriteResult::PersistedValue(b) => return Ok(Json(b)),
        WriteResult::Persisted(r) => r,
        WriteResult::Response { status: _, body } => return Ok(Json(body)),
    };

    reconcile_owner_refs_after_mutation(&state, &resource, "namespaced_patch").await;

    dispatch_pod_handler_mutation_event(
        &state,
        klights_reconcile_api::MutationOperation::Patch,
        &resource.data,
        "pod_patch",
    )
    .await;

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

fn content_type_to_patch_type(content_type: Option<&str>) -> klights_pod_api::PodStatusPatchKind {
    use klights_pod_api::PodStatusPatchKind;
    match content_type {
        Some("application/json-patch+json") => PodStatusPatchKind::JsonPatch,
        Some("application/strategic-merge-patch+json") => PodStatusPatchKind::StrategicMerge,
        Some("application/apply-patch+yaml") => PodStatusPatchKind::ApplyPatch,
        // Default (application/merge-patch+json, application/json, missing) → MergePatch
        _ => PodStatusPatchKind::MergePatch,
    }
}

pub(in crate::api) async fn delete_collection_pods(
    State(state): State<Arc<ApiState>>,
    Path(namespace): Path<String>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let dry_run = crate::api::mutation::DryRunMode::from_delete_collection_query(&query)?;
    let is_dry_run = dry_run.is_all();
    klights_pod_api::PodApiMutation::delete_collection_pods(
        state.resource_mutation().pod_repository.as_ref(),
        klights_pod_api::PodApiDeleteCollectionRequest {
            namespace: namespace.clone(),
            label_selector: query.label_selector,
            field_selector: None, // matches today's macro behavior
            dry_run: is_dry_run,
        },
    )
    .await?;

    // P3d-1: post-bulk-delete side effects (RQ recount mainly). The RQ hook
    // only needs metadata.namespace, so a synthesized stub is enough.
    if !is_dry_run {
        let stub = serde_json::json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"namespace": namespace.clone()},
        });
        dispatch_pod_handler_mutation_event(
            &state,
            klights_reconcile_api::MutationOperation::DeleteMark,
            &stub,
            "pod_delete_collection",
        )
        .await;
    }

    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}

pub(in crate::api) async fn list_all_pods(
    State(state): State<Arc<ApiState>>,
    Query(query): Query<ListQuery>,
    headers: HeaderMap,
) -> Result<Response, AppError> {
    validate_builtin_field_selector(
        "v1",
        "Pod",
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        true,
    )?;
    // Watch streaming for cluster-wide list (all namespaces)
    if query.watch == Some("true".to_string()) {
        query.validate_send_initial_events_watch()?;
        let kind = "Pod".to_string();
        let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
        let table_format = wants_table_format(&headers)?;
        let protobuf_supported = protobuf_watch_supported_for_request(
            "v1",
            "Pod",
            table_format,
            query.label_selector.as_deref(),
            query.field_selector.as_deref(),
        );
        let stream_format = negotiate_watch_stream_format(&headers, protobuf_supported)?;
        let label_selector = query.label_selector.clone();
        let field_selector = query.field_selector.clone();

        let requested_rv: i64 = query
            .resource_version
            .as_ref()
            .and_then(|rv| rv.parse::<i64>().ok())
            .unwrap_or(0);
        let explicit_resource_version_zero = query
            .resource_version
            .as_deref()
            .is_some_and(|rv| rv.trim() == "0");

        let send_initial_events = query.send_initial_events.as_deref() == Some("true");
        let watch_stream = state.resource_mutation().watch_stream.clone();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            source: watch_stream,
            task_supervisor: state.operational().task_supervisor.clone(),
            api_version: "v1",
            kind,
            watch_namespace: None,
            requested_rv,
            send_initial_events,
            send_bookmarks,
            label_selector,
            field_selector,
            table_format,
            stream_format,
            timeout_seconds: query.timeout_seconds,
            emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
            wall_clock: state.operational().clock.clone(),
        })
        .await;
        return Ok(Response::builder()
            .header("Content-Type", stream_format.content_type())
            .header("Transfer-Encoding", "chunked")
            .body(body)
            .unwrap());
    }

    let operation_now = state.operational().clock.now();
    let normalized_limit = query.normalized_limit()?;

    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|t| !t.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;

    // Decode continue token: check TTL and extract name for DB filter.
    let (db_continue_name, continue_resource_version) =
        process_continue_token_at(query.continue_token, operation_now.unix_timestamp())?;

    let list_query = klights_pod_api::PodListRequest::try_new(
        None,
        query.label_selector.clone(),
        query.field_selector.clone(),
        normalized_limit,
        db_continue_name,
    )
    .map_err(AppError::from)?;

    // Cluster-wide Pod list: same consistent-snapshot path as the namespaced
    // handler, with no namespace scope. See `query::resolve_list_page`.
    let pod_repository = state.resource_mutation().pod_repository.clone();
    let snapshot_repository = pod_repository.clone();
    let snapshot_query = list_query.clone();
    let live_query = list_query;
    let crate::api::query::ResolvedListPage {
        list,
        response_rv,
        continue_resource_version,
    } = crate::api::query::resolve_list_page(
        state.resource_mutation().list_resource_versions.as_ref(),
        rv_match,
        continue_resource_version,
        |srv| async move {
            klights_pod_api::PodSnapshotQuery::snapshot_pods(
                snapshot_repository.as_ref(),
                klights_pod_api::PodSnapshotListRequest {
                    list: snapshot_query,
                    snapshot_resource_version: srv,
                },
            )
            .await
            .map_err(AppError::from)
        },
        || async move {
            klights_pod_api::PodQuery::list_pods(pod_repository.as_ref(), live_query)
                .await
                .map_err(AppError::from)
        },
    )
    .await?;

    let (list_items, _, list_continue_token, remaining_item_count) = list.into_parts();
    let items: Vec<Value> = list_items
        .into_iter()
        .map(|r| inject_resource_version(r.data, r.resource_version))
        .collect();
    let resource_version = response_rv.to_string();

    // Return Table format if requested by kubectl
    if wants_table_format(&headers)? {
        let table = pod_list_to_table_at(items, resource_version, operation_now);
        return Ok(Json(table).into_response());
    }

    // Return normal List format
    // Omit "continue" when None; include "remainingItemCount" only when paginating.
    let mut metadata = serde_json::json!({
        "resourceVersion": resource_version,
    });
    if let Some(ref name) = list_continue_token {
        let token = crate::api::query::encode_response_continue_token_at(
            name,
            response_rv,
            continue_resource_version,
            operation_now.unix_timestamp(),
        );
        metadata["continue"] = serde_json::json!(token);
    }
    if let Some(remaining) = remaining_item_count {
        metadata["remainingItemCount"] = serde_json::json!(remaining);
    }
    let response = serde_json::json!({
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": metadata,
        "items": items,
    });

    Ok(K8sResponse::new(response, &headers).into_response())
}

// ---------------------------------------------------------------------------
// Pod write strategies
//
// Wrap the handler-owned pre-flight checks (namespace guard, dry-run parsing,
// strict field validation) and delegate persistence to `PodApiWriter`. Pod
// admission, validation, quota, status preservation, and actor-finalize
// enqueue stay inside `PodApiWriter` (no admission duplication here).
// ---------------------------------------------------------------------------

struct PodCreateStrategy<'a> {
    state: &'a Arc<ApiState>,
    namespace: &'a str,
    query: &'a CreateUpdateQuery,
}

struct PodUpdateStrategy<'a> {
    state: &'a Arc<ApiState>,
    namespace: &'a str,
    name: &'a str,
    query: &'a CreateUpdateQuery,
}

struct PodPatchStrategy<'a> {
    state: &'a Arc<ApiState>,
    namespace: &'a str,
    name: &'a str,
    query: &'a CreateUpdateQuery,
    headers: &'a HeaderMap,
}

#[async_trait]
impl CreateStrategy for PodCreateStrategy<'_> {
    async fn before_admission(&self, body: Value) -> Result<Value, AppError> {
        self.state
            .resource_mutation()
            .builtin_admission_defaults
            .ensure_namespace_active(self.namespace.to_string())
            .await?;
        check_field_validation_strict_typed("v1", "Pod", self.query, &body)?;
        Ok(body)
    }

    async fn admit(
        &self,
        body: Value,
        _dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<Value, AppError> {
        // Admission stays inside `PodApiWriter::api_create_pod`.
        Ok(body)
    }

    async fn persist_create(
        &self,
        body: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let resource_name = body
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let result = klights_pod_api::PodApiMutation::create_pod(
            self.state.resource_mutation().pod_repository.as_ref(),
            klights_pod_api::PodApiCreateRequest {
                namespace: self.namespace.to_string(),
                body,
                dry_run: dry_run.is_all(),
            },
        )
        .await
        .map_err(|error| {
            AppError::from(error).with_resource_context("v1", "Pod", &resource_name)
        })?;
        Ok(match result.resource {
            Some(resource) => WriteResult::Persisted(resource),
            None => WriteResult::DryRun(result.body),
        })
    }
}

#[async_trait]
impl UpdateStrategy for PodUpdateStrategy<'_> {
    async fn load_current(&self) -> Result<klights_cluster_core::Resource, AppError> {
        crate::api::pod_repository_ports::get_pod(
            self.state.resource_mutation().pod_repository.as_ref(),
            self.namespace,
            self.name,
        )
        .await?
        .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))
    }

    async fn prepare_update(
        &self,
        _current: &klights_cluster_core::Resource,
        body: Value,
        _dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<Value, AppError> {
        check_field_validation_strict_typed("v1", "Pod", self.query, &body)?;
        Ok(body)
    }

    async fn persist_update(
        &self,
        current: klights_cluster_core::Resource,
        body: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let outcome = klights_pod_api::PodApiMutation::update_pod(
            self.state.resource_mutation().pod_repository.as_ref(),
            klights_pod_api::PodApiUpdateRequest {
                namespace: self.namespace.to_string(),
                name: self.name.to_string(),
                body,
                current,
                dry_run: dry_run.is_all(),
            },
        )
        .await?;
        Ok(match outcome {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                WriteResult::Persisted(resource)
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => WriteResult::DryRun(value),
        })
    }
}

#[async_trait]
impl PatchStrategy for PodPatchStrategy<'_> {
    async fn apply_patch(
        &self,
        patch: Value,
        dry_run: crate::api::mutation::DryRunMode,
    ) -> Result<WriteResult, AppError> {
        let content_type = self
            .headers
            .get("content-type")
            .and_then(|h| h.to_str().ok());

        // For server-side apply (SSA), validate fields strictly if requested
        // (deep: catches nested unknown fields like spec.bogus).
        let is_apply = content_type == Some("application/apply-patch+yaml")
            || content_type == Some("application/apply-patch+json");
        if is_apply {
            check_field_validation_strict_typed("v1", "Pod", self.query, &patch)?;
        } else if self.query.field_validation.as_deref() == Some("Strict") {
            // Non-apply patch (merge/strategic/JSON): deep-validate the *merged*
            // result so nested unknown fields are rejected under Strict, matching
            // the generic patch path. Only runs on the opt-in Strict path.
            let current = crate::api::pod_repository_ports::get_pod(
                self.state.resource_mutation().pod_repository.as_ref(),
                self.namespace,
                self.name,
            )
            .await?
            .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;
            let merged = apply_patch(&current.data, &patch, content_type)?;
            check_field_validation_strict_typed("v1", "Pod", self.query, &merged)?;
        }

        let patch_type = content_type_to_patch_type(content_type);
        let outcome = klights_pod_api::PodApiMutation::patch_pod(
            self.state.resource_mutation().pod_repository.as_ref(),
            klights_pod_api::PodApiPatchRequest {
                namespace: self.namespace.to_string(),
                name: self.name.to_string(),
                patch,
                patch_kind: patch_type,
                dry_run: dry_run.is_all(),
            },
        )
        .await?;
        Ok(match outcome {
            klights_pod_api::PodApiWriteOutcome::Persisted(resource) => {
                WriteResult::Persisted(resource)
            }
            klights_pod_api::PodApiWriteOutcome::DryRun(value) => WriteResult::DryRun(value),
        })
    }
}
