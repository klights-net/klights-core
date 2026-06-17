use crate::api::*;
use crate::datastore::ListPageRequest;
use crate::label_selector::LabelSelector;

pub async fn get_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<Json<Value>, AppError> {
    let ns = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or(AppError::NotFound(format!("namespace {} not found", name)))?;
    let data = inject_resource_version(ns.data, ns.resource_version);
    Ok(Json(data))
}

pub async fn list_namespaces(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ListQuery>,
) -> Result<Response, AppError> {
    if let Some(selector) = query
        .label_selector
        .as_deref()
        .map(str::trim)
        .filter(|selector| !selector.is_empty())
    {
        LabelSelector::parse(selector)
            .map_err(|err| AppError::BadRequest(format!("Invalid label selector: {err}")))?;
    }

    // Watch streaming for Namespaces
    if query.watch == Some("true".to_string()) {
        let send_bookmarks = query.allow_watch_bookmarks == Some("true".to_string());
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
            .subscribe_watch_signals(crate::watch::WatchTopic::new("v1", "Namespace"));
        let db = state.db.clone();
        let body = build_label_selector_watch_stream(LabelSelectorWatchStreamRequest {
            db,
            signal_rx,
            task_supervisor: state.task_supervisor.clone(),
            api_version: "v1",
            kind: "Namespace".to_string(),
            watch_namespace: None,
            requested_rv,
            send_initial_events,
            send_bookmarks,
            label_selector,
            field_selector,
            table_format: false,
            catch_up_mode: WatchCatchUpMode::ClusterOnly,
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
    let (db_continue_name, continue_resource_version) =
        process_continue_token(query.continue_token.clone())?;

    let list_query = crate::datastore::ResourceListQuery::new(
        query.label_selector.as_deref(),
        query.field_selector.as_deref(),
        normalized_limit,
        db_continue_name.as_deref(),
    );

    // Namespaces persist in their own table but back a real consistent snapshot
    // (the sqlite reconstructor reads that table when kind == Namespace), so the
    // shared helper pins paginated continuations / Exact reads exactly like every
    // other kind. See `query::resolve_list_page`.
    let db_for_snapshot = state.db.clone();
    let db_for_live = state.db.clone();
    let crate::api::query::ResolvedListPage {
        list: list_response,
        response_rv,
        continue_resource_version,
    } = crate::api::query::resolve_list_page(
        state.db.as_ref(),
        rv_match,
        continue_resource_version,
        |srv| async move {
            db_for_snapshot
                .snapshot_resources_at_rv("v1", "Namespace", None, list_query, srv)
                .await
                .map_err(AppError::from)
        },
        || async move {
            let page = ListPageRequest::try_new(
                list_query.limit,
                list_query.continue_token.map(str::to_string),
            )
            .map_err(AppError::from)?;
            db_for_live
                .list_namespaces_page(list_query.label_selector, list_query.field_selector, page)
                .await
                .map_err(AppError::from)
        },
    )
    .await?;

    let items_with_rv: Vec<Value> = list_response
        .items
        .into_iter()
        .map(|r| inject_resource_version(r.data, r.resource_version))
        .collect();

    let mut ns_metadata = serde_json::json!({
        "resourceVersion": response_rv.to_string(),
    });
    if let Some(ref token) = list_response.continue_token {
        ns_metadata["continue"] =
            serde_json::json!(crate::api::query::encode_response_continue_token(
                token,
                response_rv,
                continue_resource_version,
            ));
    }
    if let Some(remaining) = list_response.remaining_item_count {
        ns_metadata["remainingItemCount"] = serde_json::json!(remaining);
    }
    let list = serde_json::json!({
        "apiVersion": "v1",
        "kind": "NamespaceList",
        "metadata": ns_metadata,
        "items": items_with_rv
    });

    Ok(Json(list).into_response())
}

pub async fn create_namespace(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(mut body): LenientJson<Value>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    let is_dry_run = query.dry_run == Some("All".to_string());
    body = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "v1",
            kind: "Namespace",
            operation: "CREATE",
            namespace: None,
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
        }),
    )
    .await?;

    if is_dry_run {
        return Ok((StatusCode::CREATED, Json(body)));
    }

    // Extract name
    let name = body
        .get("metadata")
        .and_then(|m| m.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| AppError::BadRequest("Missing metadata.name".to_string()))?
        .to_string();

    // Inject metadata fields
    if let Some(obj) = body.as_object_mut() {
        if let Some(metadata) = obj.get_mut("metadata")
            && let Some(meta_obj) = metadata.as_object_mut()
        {
            meta_obj.insert("name".to_string(), serde_json::Value::String(name.clone()));
            let uid_missing_or_empty = meta_obj
                .get("uid")
                .is_none_or(|v| v.is_null() || v.as_str().is_some_and(|s| s.trim().is_empty()));
            if uid_missing_or_empty {
                meta_obj.insert(
                    "uid".to_string(),
                    serde_json::Value::String(uuid::Uuid::new_v4().to_string()),
                );
            }
            if meta_obj
                .get("creationTimestamp")
                .is_none_or(|v| v.is_null())
            {
                meta_obj.insert(
                    "creationTimestamp".to_string(),
                    serde_json::Value::String(crate::utils::k8s_timestamp()),
                );
            }
        }
        let spec = obj
            .entry("spec".to_string())
            .or_insert_with(|| serde_json::json!({}));
        if let Some(spec_obj) = spec.as_object_mut() {
            let needs_default_finalizer = spec_obj
                .get("finalizers")
                .and_then(|v| v.as_array())
                .is_none_or(|v| v.is_empty());
            if needs_default_finalizer {
                spec_obj.insert("finalizers".to_string(), serde_json::json!(["kubernetes"]));
            }
        }
    }
    ensure_namespace_status_phase_active(&mut body);

    let resource = state
        .db
        .create_namespace(&name, body)
        .await
        .map_err(|err| map_namespace_create_error(&name, err))?;

    // Auto-create default ServiceAccount and kube-root-ca.crt ConfigMap
    if let Err(e) =
        crate::controllers::namespace::create_default_service_account(state.db.as_ref(), &name)
            .await
    {
        tracing::warn!(
            "Failed to create default ServiceAccount in namespace {}: {:#}",
            name,
            e
        );
    }
    let ca_cert_path = crate::paths::ca_cert_path(&state.config.containerd_namespace);
    match crate::utils::read_utf8_file_async(&ca_cert_path).await {
        Ok(ca_cert_pem) => {
            if let Err(e) = crate::controllers::namespace::create_kube_root_ca_configmap(
                state.db.as_ref(),
                &name,
                &ca_cert_pem,
            )
            .await
            {
                tracing::warn!(
                    "Failed to create kube-root-ca.crt ConfigMap in namespace {}: {:#}",
                    name,
                    e
                );
            }
        }
        Err(e) => {
            // Log at error level — missing kube-root-ca.crt causes conformance failures
            // (sig-auth: ServiceAccounts should guarantee kube-root-ca.crt in any namespace).
            tracing::error!(
                "Cannot create kube-root-ca.crt ConfigMap in namespace {}: \
                 failed to read CA cert from {}: {}. \
                 Ensure klights was initialized and the configured data directory exists.",
                name,
                ca_cert_path.display(),
                e
            );
        }
    }

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok((StatusCode::CREATED, Json(data)))
}

fn map_namespace_create_error(name: &str, err: anyhow::Error) -> AppError {
    if err.to_string().contains("already exists") {
        AppError::AlreadyExists(format!("namespaces \"{}\" already exists", name))
    } else {
        AppError::from(err)
    }
}

pub async fn update_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(mut body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let current = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;

    let is_dry_run = query.dry_run == Some("All".to_string());
    body = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "v1",
            kind: "Namespace",
            operation: "UPDATE",
            namespace: None,
            name: Some(name.clone()),
            object: body,
            old_object: Some((*current.data).clone()),
            dry_run: is_dry_run,
            subresource: None,
            options: None,
        }),
    )
    .await?;

    if is_dry_run {
        return Ok(Json(body));
    }

    let resource = state
        .db
        .update_namespace(&name, body, current.resource_version)
        .await?;
    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

pub async fn finalize_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    Query(query): Query<CreateUpdateQuery>,
    LenientJson(body): LenientJson<Value>,
) -> Result<Json<Value>, AppError> {
    let current = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;

    if query.dry_run == Some("All".to_string()) {
        return Ok(Json(body));
    }

    // Finalize updates the finalizers list (spec.finalizers)
    // Extract finalizers from request body and update namespace
    let resource = state
        .db
        .update_namespace(&name, body, current.resource_version)
        .await?;

    let finalizers_empty = resource
        .data
        .pointer("/spec/finalizers")
        .and_then(|v| v.as_array())
        .is_none_or(|arr| arr.is_empty());
    let has_deletion_timestamp = resource
        .data
        .pointer("/metadata/deletionTimestamp")
        .and_then(|v| v.as_str())
        .is_some();
    if finalizers_empty && has_deletion_timestamp {
        if state.db.count_namespace_resources(&resource.name).await? == 0 {
            state.db.delete_namespace(&resource.name.clone()).await?;
        } else {
            let uid = resource
                .data
                .pointer("/metadata/uid")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // Use the outcome-returning variant: under churn the inner
            // reconcile may return Ok while the namespace is still
            // Terminating (pods still draining, content pending). We must
            // enqueue a workqueue retry in that StillPending case too,
            // not only on Err.
            let outcome = crate::api::reconcile_namespace_termination_for_uid_with_outcome(
                state.db.as_ref(),
                &resource.name,
                &uid,
                &state.metrics,
            )
            .await;
            let need_retry = match &outcome {
                Ok(crate::api::NamespaceTerminationOutcome::Finalized) => false,
                Ok(crate::api::NamespaceTerminationOutcome::StillPending) => true,
                Err(err) => {
                    tracing::error!(
                        namespace = %resource.name,
                        error = ?err,
                        "namespace finalize termination reconcile failed; enqueuing retry"
                    );
                    true
                }
            };
            if need_retry {
                crate::kubelet::pod_repository::PodRepository::enqueue_namespace_termination(
                    state.pod_repository.as_ref(),
                    resource.name.clone(),
                    uid,
                )
                .await
                .map_err(|e| {
                    AppError::Internal(format!(
                        "failed to enqueue namespace termination retry: {}",
                        e
                    ))
                })?;
            }
        }
    }

    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

pub async fn patch_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    Query(query): Query<CreateUpdateQuery>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, AppError> {
    let current = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;

    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());

    // apply_patch expects &Value for patch, but we have Bytes. Parse it first.
    let patch_value: Value = serde_json::from_slice(&body)
        .map_err(|e| AppError::BadRequest(format!("Invalid patch body: {}", e)))?;
    let patched = apply_patch(&current.data, &patch_value, content_type)?;
    let patched = run_admission_for_request(
        state.db.as_ref(),
        build_admission_context(AdmissionContextRequest {
            api_version: "v1",
            kind: "Namespace",
            operation: "UPDATE",
            namespace: None,
            name: Some(name.clone()),
            object: patched,
            old_object: Some((*current.data).clone()),
            dry_run: query.dry_run == Some("All".to_string()),
            subresource: None,
            options: None,
        }),
    )
    .await?;

    if query.dry_run == Some("All".to_string()) {
        return Ok(Json(patched));
    }

    let resource = state
        .db
        .update_namespace(&name, patched, current.resource_version)
        .await?;
    let data = inject_resource_version(resource.data, resource.resource_version);
    Ok(Json(data))
}

/// System namespaces that upstream forbids deleting. Mirrors the
/// `NamespaceLifecycle` admission plugin's protected set.
const PROTECTED_NAMESPACES: [&str; 4] =
    ["default", "kube-system", "kube-public", "kube-node-lease"];

/// Returns true when `name` is a system namespace that may not be deleted.
pub fn is_protected_namespace(name: &str) -> bool {
    PROTECTED_NAMESPACES.contains(&name)
}

pub async fn delete_namespace(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Result<(StatusCode, Json<Value>), AppError> {
    if is_protected_namespace(&name) {
        return Err(AppError::Forbidden(format!(
            "namespace {} is reserved by the system and cannot be deleted",
            name
        )));
    }

    let current = state
        .db
        .get_namespace(&name)
        .await?
        .ok_or_else(|| AppError::NotFound(format!("Namespace {} not found", name)))?;

    let has_finalizers = resource_has_finalizers(&current.data, "/spec/finalizers");
    if !has_finalizers && state.db.count_namespace_resources(&name).await? == 0 {
        state.db.delete_namespace(&name).await?;
        return Ok((
            StatusCode::OK,
            Json(serde_json::json!({
                "apiVersion": "v1",
                "kind": "Status",
                "status": "Success",
                "code": 200,
            })),
        ));
    }

    let mut terminating: Value = (*current.data).clone();
    set_namespace_terminating_status(&mut terminating, false);
    let updated = state
        .db
        .update_namespace(&name, terminating, current.resource_version)
        .await?;
    let uid = updated
        .data
        .pointer("/metadata/uid")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    // Use the outcome-returning variant: when the inner reconcile returns
    // Ok with StillPending (pods still draining, content not yet drained,
    // or worker-side races), enqueue a workqueue retry — not only on Err.
    let outcome = crate::api::reconcile_namespace_termination_for_uid_with_outcome(
        state.db.as_ref(),
        &name,
        &uid,
        &state.metrics,
    )
    .await;
    let need_retry = match &outcome {
        Ok(crate::api::NamespaceTerminationOutcome::Finalized) => false,
        Ok(crate::api::NamespaceTerminationOutcome::StillPending) => true,
        Err(err) => {
            tracing::error!(
                namespace = %name,
                error = ?err,
                "namespace termination first-attempt failed; enqueuing retry"
            );
            true
        }
    };
    if need_retry {
        crate::kubelet::pod_repository::PodRepository::enqueue_namespace_termination(
            state.pod_repository.as_ref(),
            name.clone(),
            uid,
        )
        .await
        .map_err(|e| {
            AppError::Internal(format!(
                "failed to enqueue namespace termination retry: {}",
                e
            ))
        })?;
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "apiVersion": "v1",
            "kind": "Status",
            "status": "Success",
            "code": 202,
        })),
    ))
}

#[cfg(test)]
mod protected_namespace_tests {
    use super::is_protected_namespace;

    #[test]
    fn protected_system_namespaces_are_guarded() {
        for name in ["default", "kube-system", "kube-public", "kube-node-lease"] {
            assert!(is_protected_namespace(name), "{name} must be protected");
        }
    }

    #[test]
    fn ordinary_namespaces_are_not_protected() {
        for name in ["my-app", "kube-systemx", "defaultt", "", "argocd"] {
            assert!(
                !is_protected_namespace(name),
                "{name} must not be protected"
            );
        }
    }
}
