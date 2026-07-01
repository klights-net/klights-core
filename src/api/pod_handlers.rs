//! Dedicated HTTP handlers for `v1/Pod`.
//!
//! Extracted verbatim from the `namespaced_resource_handlers!` and
//! `cluster_wide_list_handler!` macros (Task 11 Step A — pure refactor, no
//! behavior change). Subsequent tasks (Step B onward) will route the create
//! path through `PodApiService::api_create_pod`; for now this file mirrors the
//! macro expansion bit-for-bit.

use crate::api::*;

pub async fn list_pods(
    State(state): State<Arc<AppState>>,
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
        let label_selector = query.label_selector.clone();
        let field_selector = query.field_selector.clone();

        // Parse resourceVersion filter (0 or missing = send all, >0 = filter old events)
        let mut requested_rv: i64 = query
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
        let has_selector = label_selector
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || field_selector
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());

        // Selector-less rv-less watches pin the live floor to "now" (the
        // pre-subscribe global rv) so the stream starts from the present.
        // Selector watches keep the floor at 0 and dedup the baseline by exact
        // rv (see build_label_selector_watch_stream), so no floor is needed.
        if requested_rv <= 0
            && !send_initial_events
            && !has_selector
            && !explicit_resource_version_zero
            && let Ok(floor) = state.db.get_current_resource_version().await
            && floor > 0
        {
            requested_rv = floor;
        }

        let signal_rx = state
            .db
            .subscribe_watch_signals(crate::watch::WatchTopic::new("v1", &kind));
        let db = state.db.clone();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            db,
            signal_rx,
            task_supervisor: state.task_supervisor.clone(),
            api_version: "v1",
            kind,
            watch_namespace: Some(ns),
            requested_rv,
            send_initial_events,
            send_bookmarks,
            label_selector,
            field_selector,
            table_format,
            catch_up_mode: WatchCatchUpMode::NamespacedScoped,
            timeout_seconds: query.timeout_seconds,
            emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
        });
        return Ok(Response::builder()
            .header("Content-Type", "application/json")
            .header("Transfer-Encoding", "chunked")
            .body(body)
            .unwrap());
    }

    let normalized_limit = query.normalized_limit()?;

    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|t| !t.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;

    // Decode continue token: check TTL and extract name for DB filter.
    let (db_continue_name, continue_resource_version) =
        process_continue_token(query.continue_token)?;

    let list_query = crate::datastore::ResourceListQuery::new(
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        normalized_limit,
        db_continue_name.as_deref(),
    );

    // Pin paginated continuations / Exact reads to a consistent snapshot, shared
    // with every other list handler. Pods live in the generic resource table, so
    // the snapshot side reads `("v1","Pod")` directly; the live side stays on the
    // PodReader port. See `query::resolve_list_page`.
    let db_for_snapshot = state.db.clone();
    let pod_repository = state.pod_repository.clone();
    let ns_for_snapshot = namespace.clone();
    let ns_for_live = namespace.clone();
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
                .snapshot_resources_at_rv("v1", "Pod", Some(&ns_for_snapshot), list_query, srv)
                .await
                .map_err(AppError::from)
        },
        || async move {
            crate::kubelet::pod_repository::PodReader::list_pods(
                pod_repository.as_ref(),
                Some(&ns_for_live),
                list_query.label_selector,
                list_query.field_selector,
                list_query.limit,
                list_query.continue_token,
            )
            .await
            .map_err(AppError::from)
        },
    )
    .await?;

    let items: Vec<Value> = list
        .items
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
        let table = pod_list_to_table(items, resource_version);
        return Ok(Json(table).into_response());
    }

    // Return normal List format
    // Omit "continue" when None; include "remainingItemCount" only when paginating.
    let mut metadata = serde_json::json!({
        "resourceVersion": resource_version,
    });
    if let Some(ref name) = list.continue_token {
        // Normal pages keep the session RV; inconsistent recovery pages must
        // keep returning inconsistent tokens.
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
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": metadata,
        "items": items,
    });

    Ok(K8sResponse::new(response, &headers).into_response())
}

pub async fn get_pod(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<K8sResponse, AppError> {
    match crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
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

pub async fn create_pod(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(body): LenientJson<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    reject_if_namespace_missing_or_terminating(state.db.as_ref(), &namespace).await?;

    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let is_dry_run = dry_run.is_all();

    // Strict field validation (deep: catches nested unknown fields like spec.bogus)
    check_field_validation_strict_typed("v1", "Pod", &query, &body)?;

    let result = crate::kubelet::pod_repository::PodApiWriter::api_create_pod(
        state.pod_repository.as_ref(),
        crate::kubelet::pod_repository::PodApiCreateRequest {
            namespace: namespace.clone(),
            name: String::new(),
            body,
            dry_run: is_dry_run,
            run_admission: true,
        },
    )
    .await?;

    if let Some(resource) = result.resource {
        let _ = state
            .side_effects
            .run_hooks(&resource.data, state.db.as_ref())
            .await;
        let data = inject_resource_version(resource.data, resource.resource_version);
        return Ok((StatusCode::CREATED, Json(data)));
    }

    Ok((StatusCode::CREATED, Json(result.body)))
}

pub async fn update_pod(
    State(state): State<Arc<AppState>>,
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

    let current = crate::kubelet::pod_repository::PodReader::get_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
    )
    .await?
    .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;

    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let is_dry_run = dry_run.is_all();

    // Strict field validation (deep: catches nested unknown fields like spec.bogus)
    check_field_validation_strict_typed("v1", "Pod", &query, &body)?;

    let outcome = crate::kubelet::pod_repository::PodApiWriter::api_update_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
        body,
        current,
        is_dry_run,
    )
    .await?;

    let resource = match outcome {
        crate::kubelet::pod_repository::PodApiUpdateOutcome::DryRun(b) => return Ok(Json(b)),
        crate::kubelet::pod_repository::PodApiUpdateOutcome::Persisted(r) => r,
    };

    tracing::debug!(
        "UPDATE {}/{} in {}: after db.update_resource",
        "Pod",
        name,
        namespace
    );

    reconcile_owner_refs_after_mutation(&state, &resource, "namespaced_update").await;

    // P3d-1: post-update side effects dispatched centrally.
    let _ = state
        .side_effects
        .run_hooks(&resource.data, state.db.as_ref())
        .await;

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

pub async fn delete_pod(
    State(state): State<Arc<AppState>>,
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

    let outcome = crate::kubelet::pod_repository::PodApiWriter::api_delete_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
        delete_intent.options,
        is_dry_run,
    )
    .await?;

    match outcome {
        crate::kubelet::pod_repository::PodApiDeleteOutcome::DryRun(v) => {
            Ok((StatusCode::OK, Json(v)))
        }
        crate::kubelet::pod_repository::PodApiDeleteOutcome::GracefulSet(r) => {
            // Fire side effects (ResourceQuota recount, etc.) after
            // pod deletionTimestamp is set. The pod still exists in the
            // datastore but the RQ reconciler excludes terminating pods.
            let _ = state
                .side_effects
                .run_hooks(&r.data, state.db.as_ref())
                .await;
            let result =
                crate::api::mutation::response::accepted_object(r.data, r.resource_version);
            Ok((StatusCode::ACCEPTED, Json(result)))
        }
    }
}

pub async fn patch_pod(
    State(state): State<Arc<AppState>>,
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
        crate::protobuf::decode_protobuf(&body[4..])
            .map_err(|e| AppError::BadRequest(format!("Failed to decode protobuf: {}", e)))?
    } else if content_type == Some("application/apply-patch+yaml") {
        // YAML for server-side apply
        parse_apply_yaml(&body)?
    } else {
        // Default to JSON
        serde_json::from_slice(&body)
            .map_err(|e| AppError::BadRequest(format!("Invalid JSON: {}", e)))?
    };

    // For server-side apply (SSA), validate fields strictly if requested
    // (deep: catches nested unknown fields like spec.bogus).
    let is_apply = content_type == Some("application/apply-patch+yaml")
        || content_type == Some("application/apply-patch+json");
    if is_apply {
        check_field_validation_strict_typed("v1", "Pod", &query, &patch)?;
    } else if query.field_validation.as_deref() == Some("Strict") {
        // Non-apply patch (merge/strategic/JSON): deep-validate the *merged*
        // result so nested unknown fields are rejected under Strict, matching
        // the generic patch path. Only runs on the opt-in Strict path.
        let current = crate::kubelet::pod_repository::PodReader::get_pod(
            state.pod_repository.as_ref(),
            &namespace,
            &name,
        )
        .await?
        .ok_or_else(|| AppError::NotFound("Pod not found".to_string()))?;
        let merged = apply_patch(&current.data, &patch, content_type)?;
        check_field_validation_strict_typed("v1", "Pod", &query, &merged)?;
    }

    let patch_type = content_type_to_patch_type(content_type);
    let dry_run = crate::api::mutation::DryRunMode::from_create_update_query(&query)?;
    let is_dry_run = dry_run.is_all();

    let outcome = crate::kubelet::pod_repository::PodApiWriter::api_patch_pod(
        state.pod_repository.as_ref(),
        &namespace,
        &name,
        patch,
        patch_type,
        is_dry_run,
    )
    .await?;

    let resource = match outcome {
        crate::kubelet::pod_repository::PodApiUpdateOutcome::DryRun(b) => return Ok(Json(b)),
        crate::kubelet::pod_repository::PodApiUpdateOutcome::Persisted(r) => r,
    };

    reconcile_owner_refs_after_mutation(&state, &resource, "namespaced_patch").await;

    // P3d-1: post-patch side effects dispatched centrally.
    let _ = state
        .side_effects
        .run_hooks(&resource.data, state.db.as_ref())
        .await;

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

fn content_type_to_patch_type(
    content_type: Option<&str>,
) -> crate::kubelet::pod_repository::PodStatusPatchType {
    use crate::kubelet::pod_repository::PodStatusPatchType;
    match content_type {
        Some("application/json-patch+json") => PodStatusPatchType::JsonPatch,
        Some("application/strategic-merge-patch+json") => PodStatusPatchType::StrategicMerge,
        Some("application/apply-patch+yaml") => PodStatusPatchType::ApplyPatch,
        // Default (application/merge-patch+json, application/json, missing) → MergePatch
        _ => PodStatusPatchType::MergePatch,
    }
}

pub async fn delete_collection_pods(
    State(state): State<Arc<AppState>>,
    Path(namespace): Path<String>,
    Query(query): Query<DeleteCollectionQuery>,
) -> Result<Json<Value>, AppError> {
    let dry_run = crate::api::mutation::DryRunMode::from_delete_collection_query(&query)?;
    let is_dry_run = dry_run.is_all();
    crate::kubelet::pod_repository::PodApiWriter::api_delete_collection_pods(
        state.pod_repository.as_ref(),
        &namespace,
        query.label_selector.as_deref(),
        None, // field_selector matches today's macro behavior
        is_dry_run,
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
        let _ = state.side_effects.run_hooks(&stub, state.db.as_ref()).await;
    }

    Ok(Json(
        crate::api::mutation::response::delete_collection_success_status(),
    ))
}

pub async fn list_all_pods(
    State(state): State<Arc<AppState>>,
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
        let label_selector = query.label_selector.clone();
        let field_selector = query.field_selector.clone();

        let mut requested_rv: i64 = query
            .resource_version
            .as_ref()
            .and_then(|rv| rv.parse::<i64>().ok())
            .unwrap_or(0);
        let explicit_resource_version_zero = query
            .resource_version
            .as_deref()
            .is_some_and(|rv| rv.trim() == "0");

        let send_initial_events = query.send_initial_events.as_deref() == Some("true");
        let has_selector = label_selector
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            || field_selector
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty());

        // Selector-less rv-less watches pin the live floor to "now"; selector
        // watches keep the floor at 0 and dedup the baseline by exact rv.
        if requested_rv <= 0
            && !send_initial_events
            && !has_selector
            && !explicit_resource_version_zero
            && let Ok(floor) = state.db.get_current_resource_version().await
            && floor > 0
        {
            requested_rv = floor;
        }

        let signal_rx = state
            .db
            .subscribe_watch_signals(crate::watch::WatchTopic::new("v1", &kind));
        let db = state.db.clone();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            db,
            signal_rx,
            task_supervisor: state.task_supervisor.clone(),
            api_version: "v1",
            kind,
            watch_namespace: None,
            requested_rv,
            send_initial_events,
            send_bookmarks,
            label_selector,
            field_selector,
            table_format,
            catch_up_mode: WatchCatchUpMode::NamespacedScoped,
            timeout_seconds: query.timeout_seconds,
            emit_initial_state_for_resource_version_zero: explicit_resource_version_zero,
        });
        return Ok(Response::builder()
            .header("Content-Type", "application/json")
            .header("Transfer-Encoding", "chunked")
            .body(body)
            .unwrap());
    }

    let normalized_limit = query.normalized_limit()?;

    let has_continue = query
        .continue_token
        .as_deref()
        .is_some_and(|t| !t.is_empty());
    let rv_match = query.resolve_resource_version_match(has_continue)?;

    // Decode continue token: check TTL and extract name for DB filter.
    let (db_continue_name, continue_resource_version) =
        process_continue_token(query.continue_token)?;

    let list_query = crate::datastore::ResourceListQuery::new(
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        normalized_limit,
        db_continue_name.as_deref(),
    );

    // Cluster-wide Pod list: same consistent-snapshot path as the namespaced
    // handler, with no namespace scope. See `query::resolve_list_page`.
    let db_for_snapshot = state.db.clone();
    let pod_repository = state.pod_repository.clone();
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
                .snapshot_resources_at_rv("v1", "Pod", None, list_query, srv)
                .await
                .map_err(AppError::from)
        },
        || async move {
            crate::kubelet::pod_repository::PodReader::list_pods(
                pod_repository.as_ref(),
                None, // All namespaces
                list_query.label_selector,
                list_query.field_selector,
                list_query.limit,
                list_query.continue_token,
            )
            .await
            .map_err(AppError::from)
        },
    )
    .await?;

    let items: Vec<Value> = list
        .items
        .into_iter()
        .map(|r| inject_resource_version(r.data, r.resource_version))
        .collect();
    let resource_version = response_rv.to_string();

    // Return Table format if requested by kubectl
    if wants_table_format(&headers)? {
        let table = pod_list_to_table(items, resource_version);
        return Ok(Json(table).into_response());
    }

    // Return normal List format
    // Omit "continue" when None; include "remainingItemCount" only when paginating.
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
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": metadata,
        "items": items,
    });

    Ok(K8sResponse::new(response, &headers).into_response())
}
